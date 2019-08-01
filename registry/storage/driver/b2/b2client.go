package b2

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type b2Client struct {
	hc             *http.Client
	keyID          string
	applicationKey string
	auth           *b2Auth
}

type b2Auth struct {
	apiURL                  string
	downloadURL             string
	authorizationToken      string
	absoluteMinimumPartSize int64
}

func newClient(keyID, applicationKey string) *b2Client {
	return &b2Client{
		hc:             http.DefaultClient,
		keyID:          keyID,
		applicationKey: applicationKey,
		auth:           nil,
	}
}

func (c *b2Client) Authorize(ctx context.Context) error {
	req, err := http.NewRequest("GET", "https://api.backblazeb2.com/b2api/v2/b2_authorize_account", nil)
	if err != nil {
		return err
	}

	req = req.WithContext(ctx)
	authorizaitionHeader := base64.StdEncoding.EncodeToString([]byte(c.keyID + ":" + c.applicationKey))
	req.Header.Set("Authorization", "Basic "+authorizaitionHeader)

	res, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return errorResponse(res)
	}

	var d struct {
		apiURL                  string `json:"apiUrl"`
		downloadURL             string `json:"downloadUrl"`
		authorizationToken      string `json:"authorizationToken"`
		absoluteMinimumPartSize int64  `json:"absoluteMinimumPartSize"`
	}
	err = json.NewDecoder(res.Body).Decode(&d)
	if err != nil {
		return err
	}

	c.auth = &b2Auth{
		apiURL:                  d.apiURL,
		downloadURL:             d.downloadURL,
		authorizationToken:      d.authorizationToken,
		absoluteMinimumPartSize: d.absoluteMinimumPartSize,
	}

	return nil
}

func (c *b2Client) authorizeIfNeeded(ctx context.Context) error {
	if c.auth != nil {
		return nil
	}
	return c.Authorize(ctx)
}

func (c *b2Client) Download(ctx context.Context, bucket, fileName string, offset int64) (io.ReadCloser, error) {
	err := c.authorizeIfNeeded(ctx)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("GET", c.auth.downloadURL+"/file/"+escapeBucket(bucket)+"/"+escapeFileName(fileName), nil)
	if err != nil {
		return nil, err
	}

	req = req.WithContext(ctx)
	req.Header.Set("Authorization", c.auth.authorizationToken)

	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}

	res, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}

	if (offset <= 0 && res.StatusCode != 200) || (offset > 0 && res.StatusCode != 206) {
		err = errorResponse(res)
		res.Body.Close()
		if err == errBadToken {
			// retry
			c.auth = nil
			return c.Download(ctx, bucket, fileName, offset)
		}
		return nil, err
	}

	return res.Body, nil
}

type uploadURLInfo struct {
	BucketID           string `json:"bucketId"`
	UploadURL          string `json:"uploadUrl"`
	AuthorizationToken string `json:"authorizationToken"`
}

func (c *b2Client) GetUploadURL(ctx context.Context, bucket string) (*uploadURLInfo, error) {
	err := c.authorizeIfNeeded(ctx)
	if err != nil {
		return nil, err
	}

	var reqBody struct {
		bucketID string `json:"bucketId"`
	}
	reqBody.bucketID = bucket
	reqBodyBytes, err := json.Marshal(&reqBody)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", c.auth.apiURL+"/b2api/v2/b2_get_upload_url", bytes.NewBuffer(reqBodyBytes))
	if err != nil {
		return nil, err
	}

	req = req.WithContext(ctx)
	req.Header.Set("Authorization", c.auth.authorizationToken)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	res, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		err = errorResponse(res)
		if err == errBadToken {
			// retry
			c.auth = nil
			return c.GetUploadURL(ctx, bucket)
		}
		return nil, err
	}

	var d uploadURLInfo
	err = json.NewDecoder(res.Body).Decode(&d)
	if err != nil {
		return nil, err
	}

	return &d, nil
}

func (c *b2Client) Upload(ctx context.Context, bucket, fileName string, content []byte) error {
	uploadURL, err := c.GetUploadURL(ctx, bucket)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", uploadURL.UploadURL, bytes.NewBuffer(content))
	if err != nil {
		return err
	}

	req = req.WithContext(ctx)
	req.Header.Set("Authorization", uploadURL.AuthorizationToken)
	req.Header.Set("X-Bz-File-Name", fileName)
	req.Header.Set("Content-Type", "b2/x-auto")
	req.Header.Set("X-Bz-Content-Sha1", fmt.Sprintf("%x", sha1.Sum(content)))

	res, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		err = errorResponse(res)
		return err
	}

	return nil
}

func (c *b2Client) UploadLargeFile(ctx context.Context, bucket, fileName string, chunkSize int64, append bool) (*LargeFileWriter, error) {
	var existingFileID *string
	if append {
		req, err := http.NewRequest("HEAD", c.auth.downloadURL+"/file/"+escapeBucket(bucket)+"/"+escapeFileName(fileName), nil)
		if err != nil {
			return nil, err
		}

		req = req.WithContext(ctx)
		req.Header.Set("Authorization", c.auth.authorizationToken)

		res, err := c.hc.Do(req)
		if err != nil {
			return nil, err
		}

		// no body because the request method is HEAD
		res.Body.Close()

		if res.StatusCode == 200 {
			fileIDHeader := res.Header.Get("X-Bz-File-Id")
			existingFileID = &fileIDHeader
		}
	}

	fileID, err := c.startLargeFile(ctx, bucket, fileName)
	if err != nil {
		return nil, err
	}

	if chunkSize < c.auth.absoluteMinimumPartSize {
		chunkSize = c.auth.absoluteMinimumPartSize
	}

	w := LargeFileWriter{
		c:         c,
		fileID:    fileID,
		chunkSize: chunkSize,
	}

	if existingFileID != nil {
		checksum, err := c.copyPart(ctx, *existingFileID, fileID, 1, nil)
		if err != nil {
			return nil, err
		}
		w.partChecksums = []string{checksum}
	}

	return &w, nil
}

func (c *b2Client) startLargeFile(ctx context.Context, bucket, fileName string) (string, error) {
	err := c.authorizeIfNeeded(ctx)
	if err != nil {
		return "", err
	}

	var reqBody struct {
		bucketID    string `json:"bucketId"`
		fileName    string `json:"fileName"`
		contentType string `json:"contentType"`
	}
	reqBody.bucketID = bucket
	reqBody.fileName = fileName
	reqBody.contentType = "b2/x-auto"
	reqBodyBytes, err := json.Marshal(&reqBody)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequest("POST", c.auth.apiURL+"/b2api/v2/b2_start_large_file", bytes.NewBuffer(reqBodyBytes))
	if err != nil {
		return "", err
	}

	req = req.WithContext(ctx)
	req.Header.Set("Authorization", c.auth.authorizationToken)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	res, err := c.hc.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		err = errorResponse(res)
		if err == errBadToken {
			// retry
			c.auth = nil
			return c.startLargeFile(ctx, bucket, fileName)
		}
		return "", err
	}

	var d struct {
		fileID string `json:"fileId"`
	}
	err = json.NewDecoder(res.Body).Decode(&d)
	if err != nil {
		return "", err
	}

	return d.fileID, nil
}

func (c *b2Client) copyPart(ctx context.Context, sourceFileID, largeFileID string, partNumber uint16, copyRange *string) (string, error) {
	err := c.authorizeIfNeeded(ctx)
	if err != nil {
		return "", err
	}

	var reqBody struct {
		sourceFileID string  `json:"sourceFileId"`
		largeFileID  string  `json:"largeFileId"`
		partNumber   uint16  `json:"partNumber"`
		copyRange    *string `json:"range"`
	}
	reqBody.sourceFileID = sourceFileID
	reqBody.largeFileID = largeFileID
	reqBody.partNumber = partNumber
	reqBody.copyRange = copyRange
	reqBodyBytes, err := json.Marshal(&reqBody)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequest("POST", c.auth.apiURL+"/b2api/v2/b2_copy_part", bytes.NewBuffer(reqBodyBytes))
	if err != nil {
		return "", err
	}

	req = req.WithContext(ctx)
	req.Header.Set("Authorization", c.auth.authorizationToken)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	res, err := c.hc.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		err = errorResponse(res)
		if err == errBadToken {
			// retry
			c.auth = nil
			return c.copyPart(ctx, sourceFileID, largeFileID, partNumber, copyRange)
		}
		return "", err
	}

	var d struct {
		contentSha1 string `json:"contentSha1"`
	}
	err = json.NewDecoder(res.Body).Decode(&d)
	if err != nil {
		return "", err
	}

	return d.contentSha1, nil
}

type uploadPartURLInfo struct {
	fileID             string `json:"fileId"`
	uploadURL          string `json:"uploadUrl"`
	authorizationToken string `json:"authorizationToken"`
}

func (c *b2Client) getUploadPartURL(ctx context.Context, fileID string) (*uploadPartURLInfo, error) {
	err := c.authorizeIfNeeded(ctx)
	if err != nil {
		return nil, err
	}

	var reqBody struct {
		fileID string `json:"fileId"`
	}
	reqBody.fileID = fileID
	reqBodyBytes, err := json.Marshal(&reqBody)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", c.auth.apiURL+"/b2api/v2/b2_get_upload_part_url", bytes.NewBuffer(reqBodyBytes))
	if err != nil {
		return nil, err
	}

	req = req.WithContext(ctx)
	req.Header.Set("Authorization", c.auth.authorizationToken)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	res, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		err = errorResponse(res)
		if err == errBadToken {
			// retry
			c.auth = nil
			return c.getUploadPartURL(ctx, fileID)
		}
		return nil, err
	}

	var d uploadPartURLInfo
	err = json.NewDecoder(res.Body).Decode(&d)
	if err != nil {
		return nil, err
	}

	return &d, nil
}

func (c *b2Client) cancelLargeFile(ctx context.Context, fileID string) error {
	err := c.authorizeIfNeeded(ctx)
	if err != nil {
		return err
	}

	var reqBody struct {
		fileID string `json:"fileId"`
	}
	reqBody.fileID = fileID
	reqBodyBytes, err := json.Marshal(&reqBody)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", c.auth.apiURL+"/b2api/v2/b2_cancel_large_file", bytes.NewBuffer(reqBodyBytes))
	if err != nil {
		return err
	}

	req = req.WithContext(ctx)
	req.Header.Set("Authorization", c.auth.authorizationToken)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	res, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		err = errorResponse(res)
		if err == errBadToken {
			// retry
			c.auth = nil
			return c.cancelLargeFile(ctx, fileID)
		}
		return err
	}

	return nil
}

func (c *b2Client) finishLargeFile(ctx context.Context, fileID string, partSha1Array []string) error {
	err := c.authorizeIfNeeded(ctx)
	if err != nil {
		return err
	}

	var reqBody struct {
		fileID        string   `json:"fileId"`
		partSha1Array []string `json:"partSha1Array"`
	}
	reqBody.fileID = fileID
	reqBody.partSha1Array = partSha1Array
	reqBodyBytes, err := json.Marshal(&reqBody)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", c.auth.apiURL+"/b2api/v2/b2_finish_large_file", bytes.NewBuffer(reqBodyBytes))
	if err != nil {
		return err
	}

	req = req.WithContext(ctx)
	req.Header.Set("Authorization", c.auth.authorizationToken)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	res, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		err = errorResponse(res)
		if err == errBadToken {
			// retry
			c.auth = nil
			return c.finishLargeFile(ctx, fileID, partSha1Array)
		}
		return err
	}

	return nil
}

func escapeBucket(bucket string) string {
	return url.PathEscape(bucket)
}

func escapeFileName(fileName string) string {
	split := strings.Split(fileName, "/")
	for i := range split {
		split[i] = url.PathEscape(split[i])
	}
	return strings.Join(split, "/")
}

var errBadToken = errors.New("bad authorization token")
var errNotFound = errors.New("file not found")

func errorResponse(res *http.Response) error {
	var buf bytes.Buffer
	buf.ReadFrom(res.Body)
	resBytes := buf.Bytes()

	// https://www.backblaze.com/b2/docs/calling.html#error_handling
	var errorData struct {
		code string `json:"code"`
	}
	json.Unmarshal(resBytes, &errorData)

	if errorData.code == "bad_auth_token" || errorData.code == "expired_auth_token" {
		return errBadToken
	}

	if errorData.code == "not_found" {
		return errNotFound
	}

	return fmt.Errorf("%d %s: %s", res.StatusCode, res.Status, resBytes)
}

type LargeFileWriter struct {
	c                       *b2Client
	fileID                  string
	chunkSize               int64
	partChecksums           []string
	uploadPartURL           *uploadPartURLInfo
	buf                     bytes.Buffer
	size                    int64
	isCanceled, isCommitted bool
}

func (w *LargeFileWriter) Write(p []byte) (int, error) {
	n := len(p)

	// shortcut
	if int64(n) >= w.chunkSize && w.buf.Len() == 0 {
		err := w.uploadChunk(p)
		if err != nil {
			return 0, err
		}
		w.size += int64(n)
		return n, nil
	}

	n, err := w.buf.Write(p)
	if err != nil {
		return n, err
	}

	if int64(w.buf.Len()) >= w.chunkSize {
		err := w.uploadChunk(w.buf.Bytes())
		if err != nil {
			return 0, err
		}
		w.buf.Reset()
	}

	w.size += int64(n)
	return n, nil
}

func (w *LargeFileWriter) Size() int64 {
	return w.size
}

func (w *LargeFileWriter) Cancel() error {
	if w.isCanceled {
		return nil
	}
	if w.isCommitted {
		return fmt.Errorf("already committed")
	}

	err := w.c.cancelLargeFile(context.Background(), w.fileID)
	if err != nil {
		return err
	}

	w.isCanceled = true
	return nil
}

func (w *LargeFileWriter) Commit() error {
	if w.isCommitted {
		return nil
	}
	if w.isCanceled {
		return fmt.Errorf("already canceled")
	}

	if w.buf.Len() > 0 {
		// upload last chunk
		err := w.uploadChunk(w.buf.Bytes())
		if err != nil {
			return err
		}
		w.buf.Reset()
	}

	err := w.c.finishLargeFile(context.Background(), w.fileID, w.partChecksums)
	if err != nil {
		return err
	}

	w.isCommitted = true
	return nil
}

func (w *LargeFileWriter) Close() error {
	if w.isCanceled || w.isCommitted {
		return nil
	}

	return w.Commit()
}

func (w *LargeFileWriter) uploadChunk(p []byte) error {
	if w.isCanceled || w.isCommitted {
		return fmt.Errorf("already closed")
	}

	if w.uploadPartURL == nil {
		u, err := w.c.getUploadPartURL(context.Background(), w.fileID)
		if err != nil {
			return err
		}
		w.uploadPartURL = u
	}

	req, err := http.NewRequest("POST", w.uploadPartURL.uploadURL, bytes.NewBuffer(p))
	if err != nil {
		return err
	}

	checksum := fmt.Sprintf("%x", sha1.Sum(p))
	req.Header.Set("Authorization", w.uploadPartURL.authorizationToken)
	req.Header.Set("X-Bz-Part-Number", fmt.Sprint(len(w.partChecksums)+1))
	req.Header.Set("X-Bz-Content-Sha1", checksum)

	res, err := w.c.hc.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		err = errorResponse(res)
		if err == errBadToken {
			// retry
			w.uploadPartURL = nil
			return w.uploadChunk(p)
		}
		return err
	}

	w.partChecksums = append(w.partChecksums, checksum)

	return nil
}
