import * as path from 'node:path'
import * as fs from 'node:fs/promises'
import * as http from 'node:https'
import * as resources from '@cloudscript/resources'
import { runCommand, Key } from '@cloudscript/signing'
import { bindFunctionModel } from 'cloudscript:core'

// const downloadDomain = domains.cohesible.createSubdomain('download')
const bucket = new resources.BlobStore()
const cdn = new resources.CDN({ bucket })

// TODO: turn each release into a resource
// right now the CDN can't be changed without breaking previous releases
//

type Goos = 'darwin' | 'windows' | 'linux' | 'freebsd'
type Goarch = 'arm64' |'amd64' | 'arm' | '386'

interface ReleasePlatform {
    os: Goos
    arch: Goarch
}

interface Release {
    version: string
    platforms: ReleasePlatform[]
}

interface GetVersionsResponse {
    readonly versions: Release[]
}

// FIXME: functions that are declared after `solvePermissions` is called are not visited for evaluation

async function getKeyOptional(store: resources.BlobStore, key: string) {
    try {
        return await store.get(key)
    } catch(e) {
        if ((e as any).name !== 'NoSuchKey' && (e as any).code !== 'NoSuchKey') {
            throw e
        }
    }
}

async function getReleases(target: string) {
    const data = await getKeyOptional(bucket, `${target}/releases.json`)
    const releases: Release[] = data ? JSON.parse(Buffer.from(data).toString('utf-8')) : []

    return releases
}

// Return 404 Not Found to signal that the registry does not have a provider with the given namespace and type.
async function getVersions(request: { pathParameters: { type: string, namespace: string } }): Promise<GetVersionsResponse> {
    const { namespace, type } = request.pathParameters
    const target = `${namespace}/${type}`

    return { versions: await getReleases(target) }
}

// ReferenceError: Cannot access 'getMetadataKey' before initialization
const getMetadataKey = (target: string, version: string, os: Goos, arch: Goarch) => `metadata/${target}/${version}/${os}/${arch}/info.json`

async function getReleaseMetadata(request: { pathParameters: { type: string, namespace: string, version: string, os: string, arch: string } }) {
    const { type, namespace, version, os, arch } = request.pathParameters
    const target = `${namespace}/${type}`
    const key = getMetadataKey(target, version, os as Goos, arch as Goarch)
    const data = await bucket.get(key)

    return JSON.parse(Buffer.from(data).toString('utf-8')) as ReleaseMetadata
}

// const api = new resources.HttpService({
//     domain: downloadDomain,
// })

// api.addRoute('GET /v1/{namespace}/{type}/versions', getVersions)
// api.addRoute('GET /v1/{namespace}/{type}/{version}/download/{os}/{arch}', getReleaseMetadata)

async function getArtifacts(outdir: string): Promise<Artifact[]> {
    return JSON.parse(await fs.readFile(path.resolve(outdir, 'artifacts.json'), 'utf8'))
}

bindFunctionModel(getArtifacts, () => [{ 
    goarch: 'arm64',
    goos: 'darwin',
    path: '',
    type: 'Binary',
    name: '*',
} as const])

async function getMetadata(outdir: string): Promise<{ project_name: string, version: string, commit: string }> {
    return JSON.parse(await fs.readFile(path.resolve(outdir, 'metadata.json'), 'utf8'))
}

async function uploadArtifact(rootdir: string, version: string, artifact: Artifact) {
    const key = `artifact/${version}/${artifact.name}`
    const artifactPath = path.resolve(rootdir, artifact.path)

    console.log(`Uploading ${artifactPath} to ${key}`)
    await bucket.put(key, await fs.readFile(artifactPath))

    return `${cdn.url}/${key}`
}


async function uploadMetadata(target: string, version: string, metadata: ReleaseMetadata) {
    const key = getMetadataKey(target, version, metadata.os, metadata.arch)

    console.log(`Uploading metadata to ${key}`)
    await bucket.put(key, Buffer.from(
        JSON.stringify(metadata),
        'utf-8'
    ))

    return `${cdn.url}/${key}`
}


async function updateReleases(target: string, release: Release) {
    console.log('Updating releases')

    const releases = await getReleases(target)
    if (releases.find(r => r.version === release.version)) {
        throw new Error(`Duplicate release: ${release.version}`)
    }

    releases.push(release)

    await bucket.put(`${target}/releases.json`, Buffer.from(
        JSON.stringify(releases),
        'utf-8'
    ))
}

interface ArtifactBase {
    readonly name: string
    readonly path: string // relative to root 
}

interface ChecksumArtifact extends ArtifactBase {
    readonly type: 'Checksum'
}

interface SignatureArtifact extends ArtifactBase {
    readonly type: 'Signature'
}

interface ArchiveArtifact extends ArtifactBase {
    readonly type: 'Archive'
    readonly goos: Goos
    readonly goarch: Goarch
    readonly extra: {
        readonly Binaries?: string[]
        readonly Checksum: string
    }
}

interface BinaryArtifact extends ArtifactBase {
    readonly goos: Goos
    readonly goarch: Goarch
    readonly type: 'Binary'
}

type Artifact = ArchiveArtifact | ChecksumArtifact | SignatureArtifact | BinaryArtifact

interface ReleaseMetadata {
    os: Goos
    arch: Goarch
    filename: string
    download_url: string // can be relative to the path
    shasums_url: string
    shasums_signature_url: string 
    shasum: string
    signing_keys: {
        gpg_public_keys: {
            key_id: string // UPPERCASE
            ascii_armor: string
            trust_signature?: string
            source?: string
            source_url?: string
        }[]
    }
}

interface ReleaseOptions {
    goReleaserPath?: string
    projectDir?: string
}

async function getGoPath() {
    const result = await runCommand('go', ['env', 'GOPATH'], {
        stdio: 'pipe',
    })

    return result.stdout.trim()
}

async function getGoReleaserPath() {
    const goPath = await getGoPath()
    const goreleaser = path.resolve(goPath, 'bin', 'goreleaser')
    await fs.access(goreleaser, fs.constants.F_OK | fs.constants.X_OK)

    return goreleaser
}

// go install github.com/goreleaser/goreleaser@latest
export async function getOrInstallGoReleaser() {
    try {
        return await getGoReleaserPath()
    } catch (e) {
        console.log('Unable to find `goreleaser`, trying to install...')
        await runCommand('go', ['install', 'github.com/goreleaser/goreleaser@latest'])

        return getGoReleaserPath()
    }
}

const keyBucket = new resources.Bucket()
const signingKey = new Key({
    keyBucket,
    name: 'release',
    domain: 'cohesible.com',
})

export function createReleaser(
    signingKey: Key
) {
    async function release(namespace: string, type: string, opt?: ReleaseOptions) {
        // XXX: the permissions solver doesn't remember where `keyBucket` came from when passing it into the resource ctor
        const { publicKeyArmor } = await signingKey.importKeys(keyBucket)

        const target = `${namespace}/${type}`
        const args = process.env['USE_SNAPSHOT']
            ? ['release', '--snapshot', '--clean']
            : ['release', '--clean']

        const rootdir = opt?.projectDir ?? process.cwd()
        const goReleaserPath = opt?.goReleaserPath ?? await getOrInstallGoReleaser()
        await runCommand(goReleaserPath, args, {
            cwd: rootdir,
            env: {
                ...process.env,
                GPG_FINGERPRINT: signingKey.fingerprint
            }
        })
    

        const outdir = path.resolve(rootdir, 'dist')
        const artifacts = await getArtifacts(outdir)
        const metadata = await getMetadata(outdir)
    
        const checksum = artifacts.find(a => a.type === 'Checksum')
        if (!checksum) {
            throw new Error('No checksum found')
        }
    
        const signature = artifacts.find(a => a.type === 'Signature')
        if (!signature) {
            throw new Error('No signature found')
        }
    
        const shasumsUrl = await uploadArtifact(rootdir, metadata.version, checksum)
        const signatureUrl = await uploadArtifact(rootdir, metadata.version, signature)
    
        const signingInfo: ReleaseMetadata['signing_keys']['gpg_public_keys'][0] = {
            key_id: signingKey.fingerprint,
            ascii_armor: publicKeyArmor,
        }
    
        const platforms: ReleasePlatform[] = []
    
        const archives = artifacts.filter((a): a is ArchiveArtifact => a.type === 'Archive')
    
        await Promise.all(archives.map(async a => {
            const platform = { os: a.goos, arch: a.goarch }
            platforms.push(platform)
    
            const downloadUrl = await uploadArtifact(rootdir, metadata.version, a)
            const artifactMetadata: ReleaseMetadata = {
                ...platform,
                filename: a.name,
                download_url: downloadUrl,
                shasums_url: shasumsUrl,
                shasums_signature_url: signatureUrl,
                shasum: a.extra.Checksum.replace(/^sha[\d]+:/, ''),
                signing_keys: {
                    gpg_public_keys: [signingInfo]
                }
            }
    
            await uploadMetadata(target, metadata.version, artifactMetadata)
        }))
    
        await updateReleases(target, {
            version: metadata.version,
            platforms
        })
    }
    
    return release
}


const release = createReleaser(signingKey)

const namespace = 'cohesible'
const type = 'terraform'

export async function main(projectDir?: string) {
    return release(namespace, type, { projectDir })
}

// npm run deploy -- --target scaffolding_closure.default
// fingerprint: 79AAEDB2F880C4EF057675F7DA9B2690E5A4C240

function getPlatform(): Goos {
    const platform = process.platform

    switch (platform) {
        case 'freebsd':
        case 'darwin':
        case 'linux':
            return platform
        case 'win32':
            return 'windows'
        default:
            throw new Error(`Unsupported operating system: ${platform}`)
    }
}

function getArch(): Goarch {
    const arch = process.arch

    switch (arch) {
        case 'arm':
        case 'arm64':
            return arch
        case 'x64':
            return 'amd64'
        case 'ia32':
            return '386'
        default:
            throw new Error(`Unsupported operating system: ${arch}`)
    }
}


export async function getDownloadUrl(opt: { version?: string; os?: Goos; arch?: Goarch } = {}) {
    const { 
        version = '*',
        os = getPlatform(),
        arch = getArch(),
    } = opt

    const target = `${namespace}/${type}`
    const releases = await getReleases(target)
    const versionPattern = new RegExp(version.replace(/\./g, '\\.').replace(/\*/g, '.'))

    const matches = releases.filter(r => !!r.platforms.find(p => p.arch === arch && p.os === os) && r.version.match(versionPattern))
    if (matches.length === 0) {
        throw new Error(`No release found for combination: (os: ${os}; arch: ${arch}; version: ${version})`)
    }

    const foundVersion = matches[matches.length - 1].version
    const metadata = await getReleaseMetadata({
        pathParameters: {
            type,
            namespace,
            os,
            arch,
            version: foundVersion,
        }
    })

    // TODO: add logic to check signature and whatnot. Needs to be _after_ we download of course

    return {
        url: metadata.download_url,
        version: foundVersion,
    }
}

export async function streamFile(url: string, dest: string) {
    const handle = await fs.open(dest, 'w')

    return new Promise<void>((resolve, reject) => {
        const req = http.get(url, res => {
            const pipe = res.pipe(handle.createWriteStream())
            res.on('end', async () => {
                await handle.close().catch(reject)
                resolve()
            })
            res.on('error', reject)
            pipe.on('error', reject)
        })

        req.on('error', reject)
    })
}

export async function getOrInstallTerraform(dir: string) {
    const info = await getDownloadUrl()
    const binaryName = `terraform_v${info.version}`
    const dest = path.resolve(dir, '.cloudscript', 'terraform', info.version, binaryName)
    const alreadyInstalled = await fs.access(dest, fs.constants.F_OK | fs.constants.X_OK).then(() => true, () => false)
    if (alreadyInstalled) {
        return {
            version: info.version,
            path: dest,
        }
    }

    const zipDest = path.resolve(dir, '.cloudscript', 'terraform.zip')
    await fs.mkdir(path.dirname(zipDest), { recursive: true })
    await streamFile(info.url, zipDest)

    try {
        await fs.mkdir(path.dirname(dest), { recursive: true })
        await runCommand('unzip', [zipDest, binaryName, '-d', path.dirname(dest)])
    } finally {
        await fs.unlink(zipDest).catch(e => {
            if ((e as any).code !== 'ENOENT') {
                throw e
            }
        })
    }

    return {
        version: info.version,
        path: dest,
    }
}

