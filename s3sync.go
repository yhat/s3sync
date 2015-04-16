package s3sync

import (
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/mitchellh/goamz/s3"
)

type Syncer struct {
	Bucket *s3.Bucket
	Perm   s3.ACL
	Force  bool
}

func fileMD5(p string) (md5Sum string, ok bool) {
	fi, err := os.Open(p)
	if err != nil {
		return "", false
	}
	defer fi.Close()
	h := md5.New()
	if _, err = io.Copy(h, fi); err != nil {
		return "", false
	}
	return fmt.Sprintf("%x", h.Sum(nil)), true
}

func (s *Syncer) Download(key, as string) (skipped bool, err error) {
	if !s.Force {
		// attempt to get the target file's MD5
		if h, ok := fileMD5(as); ok {
			k, err := s.Bucket.GetKey(key)
			if err != nil {
				// something's wrong if we can't get the key
				return false, fmt.Errorf("download: getting key: %v", err)
			}
			// if the ETag matches the file, we're all done!
			if h == k.ETag {
				return true, nil
			}
		}
	}

	// if the destination directory does not exist, create it
	dir := filepath.Dir(as)
	if _, err = os.Stat(dir); err != nil {
		if err = os.MkdirAll(dir, 0755); err != nil {
			return false, fmt.Errorf("download: creating destination directory: %v", err)
		}
	}

	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	fi, err := os.OpenFile(as, flags, 0644)
	if err != nil {
		return false, fmt.Errorf("download: opening file: %v", err)
	}

	rc, err := s.Bucket.GetReader(key)
	if err != nil {
		return false, fmt.Errorf("download: getting object: %v", err)
	}
	defer rc.Close()

	if _, err = io.Copy(fi, rc); err != nil {
		return false, fmt.Errorf("download: downloading file: %v", err)
	}
	return false, nil
}

func (s *Syncer) Upload(file, key string) (skipped bool, err error) {
	fi, err := os.Open(file)
	if err != nil {
		return false, fmt.Errorf("upload: opening file: %v", err)
	}
	defer fi.Close()

	if !s.Force {
		k, err := s.Bucket.GetKey(key)
		if err == nil {
			h := md5.New()
			if _, err = io.Copy(h, fi); err != nil {
				return false, fmt.Errorf("upload: computing hash: %v", err)
			}
			hash := fmt.Sprintf("%x", h.Sum(nil))
			if hash == k.ETag {
				return true, nil
			}
			if _, err = fi.Seek(0, 0); err != nil {
				return false, fmt.Errorf("upload: seeking file: %v", err)
			}
		}
	}

	info, err := fi.Stat()
	if err != nil {
		return false, fmt.Errorf("upload: stating file: %v", err)
	}

	err = s.Bucket.PutReader(key, fi, info.Size(), "text/plain", s.Perm)
	if err != nil {
		return false, fmt.Errorf("upload: %v", err)
	}
	return false, nil
}
