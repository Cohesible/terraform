// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package http

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/hashicorp/go-retryablehttp"

	"github.com/hashicorp/terraform/internal/backend"
	"github.com/hashicorp/terraform/internal/command/views"
	"github.com/hashicorp/terraform/internal/legacy/helper/schema"
	"github.com/hashicorp/terraform/internal/logging"
	"github.com/hashicorp/terraform/internal/states/remote"
	"github.com/hashicorp/terraform/internal/states/statemgr"
	"github.com/hashicorp/terraform/internal/terraform"
	"github.com/hashicorp/terraform/internal/tfdiags"
)

func New() backend.Backend {
	s := &schema.Backend{
		Schema: map[string]*schema.Schema{
			"address": &schema.Schema{
				Type:        schema.TypeString,
				Required:    true,
				DefaultFunc: schema.EnvDefaultFunc("TF_HTTP_ADDRESS", nil),
				Description: "The address of the REST endpoint",
			},
			"update_method": &schema.Schema{
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("TF_HTTP_UPDATE_METHOD", "POST"),
				Description: "HTTP method to use when updating state",
			},
			"lock_address": &schema.Schema{
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("TF_HTTP_LOCK_ADDRESS", nil),
				Description: "The address of the lock REST endpoint",
			},
			"unlock_address": &schema.Schema{
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("TF_HTTP_UNLOCK_ADDRESS", nil),
				Description: "The address of the unlock REST endpoint",
			},
			"lock_method": &schema.Schema{
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("TF_HTTP_LOCK_METHOD", "LOCK"),
				Description: "The HTTP method to use when locking",
			},
			"unlock_method": &schema.Schema{
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("TF_HTTP_UNLOCK_METHOD", "UNLOCK"),
				Description: "The HTTP method to use when unlocking",
			},
			"username": &schema.Schema{
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("TF_HTTP_USERNAME", nil),
				Description: "The username for HTTP basic authentication",
			},
			"password": &schema.Schema{
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("TF_HTTP_PASSWORD", nil),
				Description: "The password for HTTP basic authentication",
			},
			"skip_cert_verification": &schema.Schema{
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
				Description: "Whether to skip TLS verification.",
			},
			"retry_max": &schema.Schema{
				Type:        schema.TypeInt,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("TF_HTTP_RETRY_MAX", 2),
				Description: "The number of HTTP request retries.",
			},
			"retry_wait_min": &schema.Schema{
				Type:        schema.TypeInt,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("TF_HTTP_RETRY_WAIT_MIN", 1),
				Description: "The minimum time in seconds to wait between HTTP request attempts.",
			},
			"retry_wait_max": &schema.Schema{
				Type:        schema.TypeInt,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("TF_HTTP_RETRY_WAIT_MAX", 30),
				Description: "The maximum time in seconds to wait between HTTP request attempts.",
			},
			"client_ca_certificate_pem": &schema.Schema{
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("TF_HTTP_CLIENT_CA_CERTIFICATE_PEM", ""),
				Description: "A PEM-encoded CA certificate chain used by the client to verify server certificates during TLS authentication.",
			},
			"client_certificate_pem": &schema.Schema{
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("TF_HTTP_CLIENT_CERTIFICATE_PEM", ""),
				Description: "A PEM-encoded certificate used by the server to verify the client during mutual TLS (mTLS) authentication.",
			},
			"client_private_key_pem": &schema.Schema{
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("TF_HTTP_CLIENT_PRIVATE_KEY_PEM", ""),
				Description: "A PEM-encoded private key, required if client_certificate_pem is specified.",
			},
		},
	}

	b := &Backend{Backend: s}
	b.Backend.ConfigureFunc = b.configure
	return b
}

type Backend struct {
	*schema.Backend

	client *httpClient
	opLock sync.Mutex

	// Terraform context. Many of these will be overridden or merged by
	// Operation. See Operation for more details.
	ContextOpts *terraform.ContextOpts

	// Cached context
	tfCtx *terraform.Context

	// OpInput will ask for necessary input prior to performing any operations.
	//
	// OpValidation will perform validation prior to running an operation. The
	// variable naming doesn't match the style of others since we have a func
	// Validate.
	OpInput      bool
	OpValidation bool
}

// configureTLS configures TLS when needed; if there are no conditions requiring TLS, no change is made.
func (b *Backend) configureTLS(client *retryablehttp.Client, data *schema.ResourceData) error {
	// If there are no conditions needing to configure TLS, leave the client untouched
	skipCertVerification := data.Get("skip_cert_verification").(bool)
	clientCACertificatePem := data.Get("client_ca_certificate_pem").(string)
	clientCertificatePem := data.Get("client_certificate_pem").(string)
	clientPrivateKeyPem := data.Get("client_private_key_pem").(string)
	if !skipCertVerification && clientCACertificatePem == "" && clientCertificatePem == "" && clientPrivateKeyPem == "" {
		return nil
	}
	if clientCertificatePem != "" && clientPrivateKeyPem == "" {
		return fmt.Errorf("client_certificate_pem is set but client_private_key_pem is not")
	}
	if clientPrivateKeyPem != "" && clientCertificatePem == "" {
		return fmt.Errorf("client_private_key_pem is set but client_certificate_pem is not")
	}

	// TLS configuration is needed; create an object and configure it
	var tlsConfig tls.Config
	client.HTTPClient.Transport.(*http.Transport).TLSClientConfig = &tlsConfig

	if skipCertVerification {
		// ignores TLS verification
		tlsConfig.InsecureSkipVerify = true
	}
	if clientCACertificatePem != "" {
		// trust servers based on a CA
		tlsConfig.RootCAs = x509.NewCertPool()
		if !tlsConfig.RootCAs.AppendCertsFromPEM([]byte(clientCACertificatePem)) {
			return errors.New("failed to append certs")
		}
	}
	if clientCertificatePem != "" && clientPrivateKeyPem != "" {
		// attach a client certificate to the TLS handshake (aka mTLS)
		certificate, err := tls.X509KeyPair([]byte(clientCertificatePem), []byte(clientPrivateKeyPem))
		if err != nil {
			return fmt.Errorf("cannot load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}

	return nil
}

func (b *Backend) configure(ctx context.Context) error {
	data := schema.FromContextBackendConfig(ctx)

	address := data.Get("address").(string)
	updateURL, err := url.Parse(address)
	if err != nil {
		return fmt.Errorf("failed to parse address URL: %s", err)
	}
	if updateURL.Scheme != "http" && updateURL.Scheme != "https" {
		return fmt.Errorf("address must be HTTP or HTTPS")
	}

	updateMethod := data.Get("update_method").(string)

	var lockURL *url.URL
	if v, ok := data.GetOk("lock_address"); ok && v.(string) != "" {
		var err error
		lockURL, err = url.Parse(v.(string))
		if err != nil {
			return fmt.Errorf("failed to parse lockAddress URL: %s", err)
		}
		if lockURL.Scheme != "http" && lockURL.Scheme != "https" {
			return fmt.Errorf("lockAddress must be HTTP or HTTPS")
		}
	}

	lockMethod := data.Get("lock_method").(string)

	var unlockURL *url.URL
	if v, ok := data.GetOk("unlock_address"); ok && v.(string) != "" {
		var err error
		unlockURL, err = url.Parse(v.(string))
		if err != nil {
			return fmt.Errorf("failed to parse unlockAddress URL: %s", err)
		}
		if unlockURL.Scheme != "http" && unlockURL.Scheme != "https" {
			return fmt.Errorf("unlockAddress must be HTTP or HTTPS")
		}
	}

	unlockMethod := data.Get("unlock_method").(string)

	rClient := retryablehttp.NewClient()
	rClient.RetryMax = data.Get("retry_max").(int)
	rClient.RetryWaitMin = time.Duration(data.Get("retry_wait_min").(int)) * time.Second
	rClient.RetryWaitMax = time.Duration(data.Get("retry_wait_max").(int)) * time.Second
	rClient.Logger = log.New(logging.LogOutput(), "", log.Flags())
	if err = b.configureTLS(rClient, data); err != nil {
		return err
	}

	b.client = &httpClient{
		URL:          updateURL,
		UpdateMethod: updateMethod,

		LockURL:      lockURL,
		LockMethod:   lockMethod,
		UnlockURL:    unlockURL,
		UnlockMethod: unlockMethod,

		Username: data.Get("username").(string),
		Password: data.Get("password").(string),

		// accessible only for testing use
		Client: rClient,
	}

	return nil
}

func (b *Backend) StateMgr(name string) (statemgr.Full, error) {
	if name != backend.DefaultStateName {
		return nil, backend.ErrWorkspacesNotSupported
	}

	return &remote.State{Client: b.client}, nil
}

func (b *Backend) Workspaces() ([]string, error) {
	return nil, backend.ErrWorkspacesNotSupported
}

func (b *Backend) DeleteWorkspace(string, bool) error {
	return backend.ErrWorkspacesNotSupported
}

// Operation implements backend.Enhanced.
func (b *Backend) Operation(ctx context.Context, op *backend.Operation) (*backend.RunningOperation, error) {
	if op.View == nil {
		panic("Operation called with nil View")
	}

	// Determine the function to call for our operation
	var f func(context.Context, context.Context, *backend.Operation, *backend.RunningOperation)
	switch op.Type {
	// case backend.OperationTypeRefresh:
	// 	f = b.opRefresh
	// case backend.OperationTypePlan:
	// 	f = b.opPlan
	case backend.OperationTypeApply:
		f = b.opApply
	default:
		return nil, fmt.Errorf(
			"unsupported operation type: %s\n\n"+
				"This is a bug in Terraform and should be reported. The local backend\n"+
				"is built-in to Terraform and should always support all operations.",
			op.Type)
	}

	// Lock
	b.opLock.Lock()

	// Build our running operation
	// the runninCtx is only used to block until the operation returns.
	runningCtx, done := context.WithCancel(context.Background())
	runningOp := &backend.RunningOperation{
		Context: runningCtx,
	}

	// stopCtx wraps the context passed in, and is used to signal a graceful Stop.
	stopCtx, stop := context.WithCancel(ctx)
	runningOp.Stop = stop

	// cancelCtx is used to cancel the operation immediately, usually
	// indicating that the process is exiting.
	cancelCtx, cancel := context.WithCancel(context.Background())
	runningOp.Cancel = cancel

	op.StateLocker = op.StateLocker.WithContext(stopCtx)

	// Do it
	go func() {
		defer logging.PanicHandler()
		defer done()
		defer stop()
		defer cancel()

		defer b.opLock.Unlock()
		f(stopCtx, cancelCtx, op, runningOp)
	}()

	// Return
	return runningOp, nil
}

func (b *Backend) opWait(
	doneCh <-chan struct{},
	stopCtx context.Context,
	cancelCtx context.Context,
	tfCtx *terraform.Context,
	opStateMgr statemgr.Persister,
	view views.Operation) (canceled bool) {
	// Wait for the operation to finish or for us to be interrupted so
	// we can handle it properly.
	select {
	case <-stopCtx.Done():
		view.Stopping()

		// try to force a PersistState just in case the process is terminated
		// before we can complete.
		if err := opStateMgr.PersistState(nil); err != nil {
			// We can't error out from here, but warn the user if there was an error.
			// If this isn't transient, we will catch it again below, and
			// attempt to save the state another way.
			var diags tfdiags.Diagnostics
			diags = diags.Append(tfdiags.Sourceless(
				tfdiags.Error,
				"Error saving current state",
				fmt.Sprintf(earlyStateWriteErrorFmt, err),
			))
			view.Diagnostics(diags)
		}

		// Stop execution
		log.Println("[TRACE] backend/local: waiting for the running operation to stop")
		go tfCtx.Stop()

		select {
		case <-cancelCtx.Done():
			log.Println("[WARN] running operation was forcefully canceled")
			// if the operation was canceled, we need to return immediately
			canceled = true
		case <-doneCh:
			log.Println("[TRACE] backend/local: graceful stop has completed")
		}
	case <-cancelCtx.Done():
		// this should not be called without first attempting to stop the
		// operation
		log.Println("[ERROR] running operation canceled without Stop")
		canceled = true
	case <-doneCh:
	}
	return
}

const earlyStateWriteErrorFmt = `Error: %s

Terraform encountered an error attempting to save the state before cancelling the current operation. Once the operation is complete another attempt will be made to save the final state.`

// backend.CLI impl.
func (b *Backend) CLIInit(opts *backend.CLIOpts) error {
	b.ContextOpts = opts.ContextOpts
	b.OpInput = opts.Input
	b.OpValidation = opts.Validation

	return nil
}
