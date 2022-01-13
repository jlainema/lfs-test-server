package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/context"
	"github.com/gorilla/mux"
)

// RequestVars contain variables from the HTTP request. Variables from routing, json body decoding, and
// some headers are stored.
type RequestVars struct {
	Oid           string
	Size          int64
	User          string
	Password      string
	Repo          string
	Authorization string
}

type BatchVars struct {
	Transfers []string       `json:"transfers,omitempty"`
	Operation string         `json:"operation"`
	Objects   []*RequestVars `json:"objects"`
}

// MetaObject is object metadata as seen by the object and metadata stores.
type MetaObject struct {
	Oid      string `json:"oid"`
	Size     int64  `json:"size"`
	Existing bool
}

type BatchResponse struct {
	Transfer string            `json:"transfer,omitempty"`
	Objects  []*Representation `json:"objects"`
}

// Representation is object medata as seen by clients of the lfs server.
type Representation struct {
	Oid     string           `json:"oid"`
	Size    int64            `json:"size"`
	Actions map[string]*link `json:"actions"`
	Error   *ObjectError     `json:"error,omitempty"`
}

type ObjectError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type User struct {
	Name string `json:"name"`
}

type Lock struct {
	Id       string    `json:"id"`
	Path     string    `json:"path"`
	Owner    User      `json:"owner"`
	LockedAt time.Time `json:"locked_at"`
}

type LockRequest struct {
	Path string `json:"path"`
}

type LockResponse struct {
	Lock    *Lock  `json:"lock"`
	Message string `json:"message,omitempty"`
}

type UnlockRequest struct {
	Force bool `json:"force"`
}

type UnlockResponse struct {
	Lock    *Lock  `json:"lock"`
	Message string `json:"message,omitempty"`
}

type LockList struct {
	Locks      []Lock `json:"locks"`
	NextCursor string `json:"next_cursor,omitempty"`
	Message    string `json:"message,omitempty"`
}

type VerifiableLockRequest struct {
	Cursor string `json:"cursor,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}

type VerifiableLockList struct {
	Ours       []Lock `json:"ours"`
	Theirs     []Lock `json:"theirs"`
	NextCursor string `json:"next_cursor,omitempty"`
	Message    string `json:"message,omitempty"`
}

type SizeResponse struct {
	Size    int64 `json:"size"`
	MaxSize int64 `json:"maxSize"`
}

type AllocatorResponse struct {
	GBx2   int64 `json:"GBx2"`
	GBx20  int64 `json:"GBx20"`
	GBx60  int64 `json:"GBx60"`
	GBx300 int64 `json:"GBx300"`
	TBx1   int64 `json:"TBx1"`
}

type ConstructorResponse struct {
	MaxSize   int64  `json:"maxSize"`
	Port      int32  `json:"port"`
	Server    string `json:"server,omitempty"`
	WritePass string `json:"writePass,omitempty"`
	ReadPass  string `json:"readPass,omitempty"`
	Note      string `json:"note,omitempty"`
}

// DownloadLink builds a URL to download the object.
func (v *RequestVars) DownloadLink() string {
	return v.internalLink("objects")
}

// UploadLink builds a URL to upload the object.
func (v *RequestVars) UploadLink(useTus bool) string {
	if useTus {
		return v.tusLink()
	}
	return v.internalLink("objects")
}

func (v *RequestVars) internalLink(subpath string) string {
	path := ""

	if server, ok := Config[v.User]; ok {
		path += fmt.Sprintf("/%s", v.User)

		if len(v.Repo) > 0 {
			path += fmt.Sprintf("/%s", v.Repo)
		}

		path += fmt.Sprintf("/%s/%s", subpath, v.Oid)

		path = fmt.Sprintf("%s://%s%s", server.Scheme, server.Host, path)
	}
	return path
}

func (v *RequestVars) tusLink() string {
	link, err := tusServer.Create(v.Oid, v.Size)
	if err != nil {
		logger.Fatal(kv{"fn": fmt.Sprintf("Unable to create tus link for %s: %v", v.Oid, err)})
	}
	return link
}

func (v *RequestVars) VerifyLink() string {
	path := ""
	if server, ok := Config[v.User]; ok {
		path += fmt.Sprintf("/%s", v.User)
		path += fmt.Sprintf("/verify/%s", v.Oid)

		path = fmt.Sprintf("%s://%s%s", server.Scheme, server.Host, path)
	}
	return path
}

// link provides a structure used to build a hypermedia representation of an HTTP link.
type link struct {
	Href      string            `json:"href"`
	Header    map[string]string `json:"header,omitempty"`
	ExpiresAt time.Time         `json:"expires_at,omitempty"`
}

// App links a Router, ContentStore, and MetaStore to provide the LFS server.
type App struct {
	router       *mux.Router
	contentStore *ContentStore
	metaStore    *MetaStore
	config       *Configuration
	currentSize  int64
	maximumSize  int64
}

// Round to block size, as filesystem is likely not packed; TODO: Detect block size
func FileSize(size int64) int64 {
	return ((size + 4095) & (-1 ^ 4095))
}

// NewApp creates a new App using the ContentStore and MetaStore provided
func NewApp(content *ContentStore, meta *MetaStore, server string) *App {
	c := newserver(server, nil)

	// logger.Log(kv{"server": c.Server})

	currentTotal, err := meta.Storage()
	if err != nil {
		return nil
	}
	msz, err := strconv.ParseInt(c.Size, 10, 64)
	if err != nil {
		return nil
	}

	app := &App{contentStore: content, metaStore: meta, config: c, currentSize: currentTotal, maximumSize: msz}
	r := mux.NewRouter()

	sp := "/" + server + "/"
	ur := sp + "{repo}/"
	app.HF(r, ur+"objects/batch", app.requireAuth(app.BatchHandler)).Methods("POST").MatcherFunc(MetaMatcher)
	route := ur + "objects/{oid}"
	app.HF(r, route, app.requireAuth(app.GetContentHandler)).Methods("GET", "HEAD").MatcherFunc(ContentMatcher)
	app.HF(r, route, app.requireAuth(app.GetMetaHandler)).Methods("GET", "HEAD").MatcherFunc(MetaMatcher)
	app.HF(r, route, app.requireAuth(app.PutHandler)).Methods("PUT").MatcherFunc(ContentMatcher)

	app.HF(r, ur+"objects", app.requireAuth(app.PostHandler)).Methods("POST").MatcherFunc(MetaMatcher)

	route = ur + "locks"
	app.HF(r, route, app.requireAuth(app.LocksHandler)).Methods("GET").MatcherFunc(MetaMatcher)
	app.HF(r, route, app.requireAuth(app.CreateLockHandler)).Methods("POST").MatcherFunc(MetaMatcher)
	app.HF(r, route+"/verify", app.requireAuth(app.LocksVerifyHandler)).Methods("POST").MatcherFunc(MetaMatcher)
	app.HF(r, route+"/{id}/unlock", app.requireAuth(app.DeleteLockHandler)).Methods("POST").MatcherFunc(MetaMatcher)

	route = sp
	app.HF(r, route+"verify/{oid}", app.requireAuth(app.VerifyHandler)).Methods("POST")
	app.HF(r, route+"size", app.requireAuth(app.CurrentSizeHandler)).Methods("POST")
	app.HF(r, route+"sizes", app.requireAuth(app.AllocatorHandler)).Methods("POST")
	app.HF(r, route+"new", app.requireAuth(app.ConstructorHandler)).Methods("PUT")

	app.addMgmt(r)

	app.router = r

	return app
}

func (a *App) HF(r *mux.Router, path string, f func(http.ResponseWriter,
	*http.Request)) *mux.Route {
	logger.Log(kv{"server": a.config.Server, "route": path})

	return r.HandleFunc(path, f)
}

func (a *App) Append(size int64) bool {
	if a.maximumSize > 0 && a.currentSize+size > a.maximumSize {
		return false
	}
	a.currentSize += FileSize(size)
	return true
}

func (a *App) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err == nil {
		context.Set(r, "RequestID", fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]))
	}
	logRequest(r, 0)
	a.router.ServeHTTP(w, r)
}

// Serve calls http.Serve with the provided Listener and the app's router
func (a *App) Serve(l net.Listener) error {
	return http.Serve(l, a)
}

// GetContentHandler gets the content from the content store
func (a *App) GetContentHandler(w http.ResponseWriter, r *http.Request) {
	rv := unpack(r)
	meta, err := a.metaStore.Get(rv)
	if err != nil {
		writeStatus(w, r, 404)
		return
	}

	// Support resume download using Range header
	var fromByte int64
	statusCode := 200
	if rangeHdr := r.Header.Get("Range"); rangeHdr != "" {
		regex := regexp.MustCompile(`bytes=(\d+)\-.*`)
		match := regex.FindStringSubmatch(rangeHdr)
		if len(match) > 1 {
			statusCode = 206
			fromByte, _ = strconv.ParseInt(match[1], 10, 64)
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", fromByte, meta.Size-1, int64(meta.Size)-fromByte))
		}
	}

	content, err := a.contentStore.Get(meta, fromByte)
	if err != nil {
		writeStatus(w, r, 404)
		return
	}
	defer content.Close()

	w.WriteHeader(statusCode)
	io.Copy(w, content)
	logRequest(r, statusCode)
}

// GetMetaHandler retrieves metadata about the object
func (a *App) GetMetaHandler(w http.ResponseWriter, r *http.Request) {
	rv := unpack(r)
	meta, err := a.metaStore.Get(rv)
	if err != nil {
		writeStatus(w, r, 404)
		return
	}

	w.Header().Set("Content-Type", metaMediaType)

	if r.Method == "GET" {
		enc := json.NewEncoder(w)
		enc.Encode(a.Represent(rv, meta, true, false, false))
	}

	logRequest(r, 200)
}

// PostHandler instructs the client how to upload data
func (a *App) PostHandler(w http.ResponseWriter, r *http.Request) {
	rv := unpack(r)
	meta, err := a.metaStore.Put(rv)
	if err != nil {
		writeStatus(w, r, 404)
		return
	}

	w.Header().Set("Content-Type", metaMediaType)

	sentStatus := 202
	if meta.Existing && a.contentStore.Exists(meta) {
		sentStatus = 200
	}
	w.WriteHeader(sentStatus)

	enc := json.NewEncoder(w)
	enc.Encode(a.Represent(rv, meta, meta.Existing, true, false))
	logRequest(r, sentStatus)
}

// BatchHandler provides the batch api
func (a *App) BatchHandler(w http.ResponseWriter, r *http.Request) {
	bv := unpackBatch(r)

	var responseObjects []*Representation

	var useTus bool
	if bv.Operation == "upload" && a.config.IsUsingTus() {
		for _, t := range bv.Transfers {
			if t == "tus" {
				useTus = true
				break
			}
		}
	}

	// Create a response object
	for _, object := range bv.Objects {
		meta, err := a.metaStore.Get(object)
		if err == nil && a.contentStore.Exists(meta) { // Object is found and exists
			responseObjects = append(responseObjects, a.Represent(object, meta, true, false, false))
			continue
		}

		// Object is not found
		if bv.Operation == "upload" {
			meta, err = a.metaStore.Put(object)
			if err == nil {
				responseObjects = append(responseObjects, a.Represent(object, meta, false, true, useTus))
			}
		} else {
			rep := &Representation{
				Oid:  object.Oid,
				Size: object.Size,
				Error: &ObjectError{
					Code:    404,
					Message: "Not found",
				},
			}
			responseObjects = append(responseObjects, rep)
		}
	}

	w.Header().Set("Content-Type", metaMediaType)

	respobj := &BatchResponse{Objects: responseObjects}
	// Respond with TUS support if advertised
	if useTus {
		respobj.Transfer = "tus"
	}

	enc := json.NewEncoder(w)
	enc.Encode(respobj)
	logRequest(r, 200)
}

// PutHandler receives data from the client and puts it into the content store
func (a *App) PutHandler(w http.ResponseWriter, r *http.Request) {
	rv := unpack(r)
	meta, err := a.metaStore.Get(rv)
	if err != nil {
		writeStatus(w, r, 404)
		return
	}

	if !a.Append(rv.Size) {
		a.metaStore.Delete(rv)
		w.WriteHeader(500)
		fmt.Fprintf(w, `{"message":"out of space for %d bytes"}`, rv.Size)
		return
	}

	if err := a.contentStore.Put(meta, r.Body); err != nil {
		a.metaStore.Delete(rv)
		w.WriteHeader(500)
		fmt.Fprintf(w, `{"message":"%s"}`, err)
		return
	}

	logRequest(r, 200)
}

func (a *App) VerifyHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	oid := vars["oid"]
	err := tusServer.Finish(oid, a.contentStore)

	if err != nil {
		logger.Fatal(kv{"fn": "VerifyHandler", "err": fmt.Sprintf("Failed to verify %s: %v", oid, err)})
	}

	logRequest(r, 200)
}

func (a *App) CurrentSizeHandler(w http.ResponseWriter, r *http.Request) {
	enc := json.NewEncoder(w)
	w.WriteHeader(http.StatusOK)
	enc.Encode(&SizeResponse{
		Size:    a.currentSize,
		MaxSize: a.maximumSize,
	})

	logRequest(r, 200)
}

func (a *App) AllocatorHandler(w http.ResponseWriter, r *http.Request) {
	enc := json.NewEncoder(w)
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", metaMediaType)
	free := a.maximumSize - a.currentSize
	GB := int64(1000000000)
	enc.Encode(&AllocatorResponse{
		GBx2:   free / (2 * GB),
		GBx20:  free / (20 * GB),
		GBx60:  free / (60 * GB),
		GBx300: free / (300 * GB),
		TBx1:   free / (1000 * GB),
	})
	logRequest(r, 200)
}

func (a *App) ConstructorHandler(w http.ResponseWriter, r *http.Request) {
	enc := json.NewEncoder(w)
	dec := json.NewDecoder(r.Body)
	w.Header().Set("Content-Type", metaMediaType)

	rq := &ConstructorResponse{}
	if err := dec.Decode(rq); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		rq.Note = "unable to decode"
		enc.Encode(&rq)
		return
	}

	free := a.maximumSize - a.currentSize
	if free < rq.MaxSize {
		w.WriteHeader(http.StatusBadRequest)
		rq.Note = "out of memory"
		enc.Encode(&rq)
		return
	}

	oq := &RequestVars{Oid: rq.Server, Size: rq.MaxSize}
	if _, err := a.metaStore.Put(oq); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		rq.Note = err.Error()
		enc.Encode(&rq)
		return
	}
	a.currentSize += rq.MaxSize

	ch := make(chan string)
	go newserver(rq.Server, ch)

	if <-ch == "up" {
		s := Config[rq.Server]
		rq.ReadPass = s.ReaderPass
		rq.WritePass = s.AdminPass
		rq.Note = strings.SplitN(s.Host, ":", 2)[0]
		if port, err := strconv.Atoi(s.Port); err != nil {
			rq.Port = int32(port)
		}
		enc.Encode(&rq)
	}

	logRequest(r, 200)
}

func (a *App) LocksHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	repo := vars["repo"]

	enc := json.NewEncoder(w)
	ll := &LockList{}

	w.Header().Set("Content-Type", metaMediaType)

	locks, nextCursor, err := a.metaStore.FilteredLocks(repo,
		r.FormValue("path"),
		r.FormValue("cursor"),
		r.FormValue("limit"))

	if err != nil {
		ll.Message = err.Error()
	} else {
		ll.Locks = locks
		ll.NextCursor = nextCursor
	}

	enc.Encode(ll)

	logRequest(r, 200)
}

func (a *App) LocksVerifyHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	repo := vars["repo"]
	user := context.Get(r, "USER")

	dec := json.NewDecoder(r.Body)
	enc := json.NewEncoder(w)

	w.Header().Set("Content-Type", metaMediaType)

	reqBody := &VerifiableLockRequest{}
	if err := dec.Decode(reqBody); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		enc.Encode(&VerifiableLockList{Message: err.Error()})
		return
	}

	// Limit is optional
	limit := reqBody.Limit
	if limit == 0 {
		limit = 100
	}

	ll := &VerifiableLockList{}
	locks, nextCursor, err := a.metaStore.FilteredLocks(repo, "",
		reqBody.Cursor,
		strconv.Itoa(limit))
	if err != nil {
		ll.Message = err.Error()
	} else {
		ll.NextCursor = nextCursor

		for _, l := range locks {
			if l.Owner.Name == user {
				ll.Ours = append(ll.Ours, l)
			} else {
				ll.Theirs = append(ll.Theirs, l)
			}
		}
	}

	enc.Encode(ll)

	logRequest(r, 200)
}

func (a *App) CreateLockHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	repo := vars["repo"]
	user := context.Get(r, "USER").(string)

	dec := json.NewDecoder(r.Body)
	enc := json.NewEncoder(w)

	w.Header().Set("Content-Type", metaMediaType)

	var lockRequest LockRequest
	if err := dec.Decode(&lockRequest); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		enc.Encode(&LockResponse{Message: err.Error()})
		return
	}

	locks, _, err := a.metaStore.FilteredLocks(repo, lockRequest.Path, "", "1")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		enc.Encode(&LockResponse{Message: err.Error()})
		return
	}
	if len(locks) > 0 {
		w.WriteHeader(http.StatusConflict)
		enc.Encode(&LockResponse{Message: "lock already created"})
		return
	}

	lock := &Lock{
		Id:       randomLockId(),
		Path:     lockRequest.Path,
		Owner:    User{Name: user},
		LockedAt: time.Now(),
	}

	if err := a.metaStore.AddLocks(repo, *lock); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		enc.Encode(&LockResponse{Message: err.Error()})
		return
	}

	w.WriteHeader(http.StatusCreated)
	enc.Encode(&LockResponse{
		Lock: lock,
	})

	logRequest(r, 200)
}

func (a *App) DeleteLockHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	repo := vars["repo"]
	lockId := vars["id"]
	user := context.Get(r, "USER").(string)

	dec := json.NewDecoder(r.Body)
	enc := json.NewEncoder(w)

	w.Header().Set("Content-Type", metaMediaType)

	var unlockRequest UnlockRequest

	if len(lockId) == 0 {
		w.WriteHeader(http.StatusBadRequest)
		enc.Encode(&UnlockResponse{Message: "invalid lock id"})
		return
	}

	if err := dec.Decode(&unlockRequest); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		enc.Encode(&UnlockResponse{Message: err.Error()})
		return
	}

	l, err := a.metaStore.DeleteLock(repo, user, lockId, unlockRequest.Force)
	if err != nil {
		if err == errNotOwner {
			w.WriteHeader(http.StatusForbidden)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		enc.Encode(&UnlockResponse{Message: err.Error()})
		return
	}
	if l == nil {
		w.WriteHeader(http.StatusNotFound)
		enc.Encode(&UnlockResponse{Message: "unable to find lock"})
		return
	}

	enc.Encode(&UnlockResponse{Lock: l})

	logRequest(r, 200)
}

// Represent takes a RequestVars and Meta and turns it into a Representation suitable
// for json encoding
func (a *App) Represent(rv *RequestVars, meta *MetaObject, download, upload, useTus bool) *Representation {
	rep := &Representation{
		Oid:     meta.Oid,
		Size:    meta.Size,
		Actions: make(map[string]*link),
	}

	header := make(map[string]string)
	verifyHeader := make(map[string]string)

	header["Accept"] = contentMediaType

	if len(rv.Authorization) > 0 {
		header["Authorization"] = rv.Authorization
		verifyHeader["Authorization"] = rv.Authorization
	}

	if download {
		rep.Actions["download"] = &link{Href: rv.DownloadLink(), Header: header}
	}

	if upload {
		rep.Actions["upload"] = &link{Href: rv.UploadLink(useTus), Header: header}
		if useTus {
			rep.Actions["verify"] = &link{Href: rv.VerifyLink(), Header: verifyHeader}
		}
	}
	return rep
}

func (a *App) requireAuth(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		logger.Log(kv{"req": r})
		if !a.config.IsPublic() {
			user, password, _ := r.BasicAuth()
			// logger.Log(kv{"user": user, "password": password})
			if user, ret := a.metaStore.Authenticate(user, password); ret == 0 || (ret == 1 && (r.Method == "PUT" || strings.Contains(r.URL.Path, "dbg"))) {
				w.Header().Set("WWW-Authenticate", "Basic realm=lfs")
				writeStatus(w, r, 401)
				return
			} else {
				context.Set(r, "USER", user)
			}
		}
		h(w, r)
	}
}

// ContentMatcher provides a mux.MatcherFunc that only allows requests that contain
// an Accept header with the contentMediaType
func ContentMatcher(r *http.Request, m *mux.RouteMatch) bool {
	mediaParts := strings.Split(r.Header.Get("Accept"), ";")
	mt := mediaParts[0]
	return mt == contentMediaType
}

// MetaMatcher provides a mux.MatcherFunc that only allows requests that contain
// an Accept header with the metaMediaType
func MetaMatcher(r *http.Request, m *mux.RouteMatch) bool {
	mediaParts := strings.Split(r.Header.Get("Accept"), ";")
	mt := mediaParts[0]
	return mt == metaMediaType
}

func randomLockId() string {
	var id [20]byte
	rand.Read(id[:])
	return fmt.Sprintf("%x", id[:])
}

func unpack(r *http.Request) *RequestVars {
	vars := mux.Vars(r)
	rv := &RequestVars{
		User:          vars["user"],
		Repo:          vars["repo"],
		Oid:           vars["oid"],
		Authorization: r.Header.Get("Authorization"),
	}

	if r.Method == "POST" { // Maybe also check if +json
		var p RequestVars
		dec := json.NewDecoder(r.Body)
		err := dec.Decode(&p)
		if err != nil {
			return rv
		}

		rv.Oid = p.Oid
		rv.Size = p.Size
	}

	return rv
}

// TODO cheap hack, unify with unpack
func unpackBatch(r *http.Request) *BatchVars {
	vars := mux.Vars(r)

	var bv BatchVars

	dec := json.NewDecoder(r.Body)
	err := dec.Decode(&bv)
	if err != nil {
		return &bv
	}

	for i := 0; i < len(bv.Objects); i++ {
		bv.Objects[i].User = vars["user"]
		bv.Objects[i].Repo = vars["repo"]
		bv.Objects[i].Authorization = r.Header.Get("Authorization")
	}

	return &bv
}

func writeStatus(w http.ResponseWriter, r *http.Request, status int) {
	message := http.StatusText(status)

	mediaParts := strings.Split(r.Header.Get("Accept"), ";")
	mt := mediaParts[0]
	if strings.HasSuffix(mt, "+json") {
		message = `{"message":"` + message + `"}`
	}

	w.WriteHeader(status)
	fmt.Fprint(w, message)
	logRequest(r, status)
}

func logRequest(r *http.Request, status int) {
	logger.Log(kv{"method": r.Method, "url": r.URL, "status": status, "request_id": context.Get(r, "RequestID")})
}
