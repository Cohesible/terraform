package command

import (
	"archive/zip"
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func addToArchive(writer *zip.Writer, name string, data []byte, info os.FileInfo) error {
	fh, err := zip.FileInfoHeader(info)
	if err != nil {
		return err
	}

	fh.Name = filepath.ToSlash(filepath.ToSlash(name))
	fh.Method = zip.Deflate
	fh.Modified = time.Time{}.UTC()
	fh.SetMode(os.FileMode(0o666))

	header, err := writer.CreateHeader(fh)
	if err != nil {
		return err
	}

	_, err = header.Write(data)
	return err
}

func CreateArchiveFromPaths(files map[string]string, dest string) error {
	f, err := os.Create(dest)
	if err != nil {
		return err
	}

	writer := zip.NewWriter(f)

	defer func() {
		writer.Close()
		f.Close()
	}()

	for name, filePath := range files {
		data, err := os.ReadFile(filePath)
		if err != nil {
			return err
		}

		info, err := os.Stat(filePath)
		if err != nil {
			return err
		}

		if err = addToArchive(writer, name, data, info); err != nil {
			return err
		}
	}

	return nil
}

type state int

const (
	Dest state = iota
	Name
	FilePath
)

func CreateZipFromStdin() error {
	scanner := bufio.NewScanner(os.Stdin)

	var dest string
	var name string
	var parseState state

	files := map[string]string{}

	for scanner.Scan() {
		token := scanner.Text()
		line := strings.TrimSpace(token)
		if line == "" {
			continue
		}

		switch parseState {
		case Dest:
			dest = line
			parseState = Name
		case Name:
			name = line
			parseState = FilePath
		case FilePath:
			files[name] = line
			parseState = Name
		}
	}

	if parseState != Name {
		return fmt.Errorf("bad parse")
	}

	if dest == "" {
		return fmt.Errorf("invalid destination, got 0-length string")
	}

	if len(files) == 0 {
		return fmt.Errorf("empty archive")
	}

	return CreateArchiveFromPaths(files, dest)
}
