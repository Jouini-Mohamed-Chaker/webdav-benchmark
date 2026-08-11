package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

// Storage abstracts "read a named blob" / "write a named blob" so the
// worker loop doesn't care whether it's hitting a local filesystem
// (ramdisk/disk) or a WebDAV server over HTTP (direct to the backend,
// or through the nginx proxy - same interface either way).
type Storage interface {
	Read(name string) ([]byte, error)
	// Write stores data under name. Implementations should make this
	// atomic from a reader's perspective (temp file + rename locally,
	// PUT-to-temp + MOVE for WebDAV) so a concurrent reader never sees
	// a half-written file.
	Write(name string, data []byte) error
	// EnsureCollection makes sure a directory/collection exists. No-op
	// where not needed (local dirs are created up front with MkdirAll).
	EnsureCollection(name string) error
	// Describe returns a short human-readable label for logging.
	Describe() string
}

// ---------------------------------------------------------------------
// Local filesystem storage
// ---------------------------------------------------------------------

type localStorage struct {
	baseDir string
}

func newLocalStorage(baseDir string) *localStorage {
	return &localStorage{baseDir: baseDir}
}

func (s *localStorage) Read(name string) ([]byte, error) {
	return os.ReadFile(filepath.Join(s.baseDir, name))
}

func (s *localStorage) Write(name string, data []byte) error {
	full := filepath.Join(s.baseDir, name)
	tmp := full + ".tmp"
	if err := os.WriteFile(tmp, data, 0o664); err != nil {
		return err
	}
	return os.Rename(tmp, full)
}

func (s *localStorage) EnsureCollection(name string) error {
	return os.MkdirAll(filepath.Join(s.baseDir, name), 0o775)
}

func (s *localStorage) Describe() string { return "local:" + s.baseDir }

// ---------------------------------------------------------------------
// WebDAV (HTTP) storage - talks to Apache mod_dav, directly or through
// the nginx proxy, depending which baseURL you point it at.
// ---------------------------------------------------------------------

type webdavStorage struct {
	baseURL string
	client  *http.Client
}

func newWebdavStorage(baseURL string, timeout time.Duration) *webdavStorage {
	baseURL = strings.TrimRight(baseURL, "/")
	return &webdavStorage{
		baseURL: baseURL,
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				// Mirrors the proxy's keepalive pool sizing so we're not
				// artificially bottlenecked on connection reuse under 20
				// concurrent sessions.
				MaxIdleConns:        256,
				MaxIdleConnsPerHost: 256,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

func (s *webdavStorage) url(name string) string {
	return s.baseURL + "/" + path.Clean(name)
}

func (s *webdavStorage) Read(name string) ([]byte, error) {
	resp, err := s.client.Get(s.url(name))
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("GET %s: unexpected status %s", name, resp.Status)
	}
	return io.ReadAll(resp.Body)
}

func (s *webdavStorage) Write(name string, data []byte) error {
	tmpName := name + ".tmp"

	req, err := http.NewRequest(http.MethodPut, s.url(tmpName), strings.NewReader(string(data)))
	if err != nil {
		return fmt.Errorf("build PUT %s: %w", tmpName, err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("PUT %s: %w", tmpName, err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("PUT %s: unexpected status %s", tmpName, resp.Status)
	}

	// Atomic-ish rename via WebDAV MOVE, mirroring the local temp+rename
	// trick so readers never see a half-written result file.
	moveReq, err := http.NewRequest("MOVE", s.url(tmpName), nil)
	if err != nil {
		return fmt.Errorf("build MOVE %s: %w", tmpName, err)
	}
	moveReq.Header.Set("Destination", s.url(name))
	moveReq.Header.Set("Overwrite", "T")
	moveResp, err := s.client.Do(moveReq)
	if err != nil {
		return fmt.Errorf("MOVE %s -> %s: %w", tmpName, name, err)
	}
	io.Copy(io.Discard, moveResp.Body)
	moveResp.Body.Close()
	if moveResp.StatusCode != http.StatusCreated && moveResp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("MOVE %s -> %s: unexpected status %s", tmpName, name, moveResp.Status)
	}
	return nil
}

func (s *webdavStorage) EnsureCollection(name string) error {
	req, err := http.NewRequest("MKCOL", s.url(name), nil)
	if err != nil {
		return fmt.Errorf("build MKCOL %s: %w", name, err)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("MKCOL %s: %w", name, err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	// 201 = created, 405 = already exists as a collection - both fine.
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusMethodNotAllowed {
		return fmt.Errorf("MKCOL %s: unexpected status %s", name, resp.Status)
	}
	return nil
}

func (s *webdavStorage) Describe() string { return "webdav:" + s.baseURL }