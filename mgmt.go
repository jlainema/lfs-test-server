package main

import (
	"fmt"
	"html/template"
	"io"
	"net/http"
	"strings"

	rice "github.com/GeertJohan/go.rice"
	"github.com/gorilla/mux"
)

var (
	cssBox      *rice.Box
	templateBox *rice.Box
)

type pageData struct {
	Name    string
	Config  *Configuration
	Users   []*MetaUser
	Objects []*MetaObject
	Locks   []Lock
	Oid     string
}

func (a *App) addMgmt(r *mux.Router) {
	route := "/" + a.config.Server + "/dbg"
	r.HandleFunc(route, basicAuth(a.indexHandler)).Methods("GET")
	r.HandleFunc(route+"/objects", basicAuth(a.objectsHandler)).Methods("GET")
	r.HandleFunc(route+"/raw/{oid}", basicAuth(a.objectsRawHandler)).Methods("GET")
	r.HandleFunc(route+"/locks", basicAuth(a.locksHandler)).Methods("GET")
	r.HandleFunc(route+"/users", basicAuth(a.usersHandler)).Methods("GET")
	r.HandleFunc(route+"/add", basicAuth(a.addUserHandler)).Methods("POST")
	r.HandleFunc(route+"/del", basicAuth(a.delUserHandler)).Methods("POST")

	cssBox = rice.MustFindBox("dbg/css")
	templateBox = rice.MustFindBox("dbg/templates")
	r.HandleFunc(route+"/css/{file}", basicAuth(cssHandler))
}

func cssHandler(w http.ResponseWriter, r *http.Request) {
	file := mux.Vars(r)["file"]
	f, err := cssBox.Open(file)
	if err != nil {
		writeStatus(w, r, 404)
		return
	}

	w.Header().Set("Content-Type", "text/css")

	io.Copy(w, f)
	f.Close()
}

func basicAuth(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		logger.Log(kv{"auth": r})
		user, pass, ok := r.BasicAuth()
		// logger.Log(kv{"user": user, "pass": pass})
		if server, found := Config[user]; found {
			ret := server.checkBasicAuth(pass, ok)

			if ret == 0 || (ret == 1 && (r.Method == "PUT" || strings.Contains(r.URL.Path, "/dbg"))) {
				w.Header().Set("WWW-Authenticate", "Basic realm=dbg")
				writeStatus(w, r, 401)
				return
			}

			h(w, r)
			logRequest(r, 200)
		} else {
			w.Header().Set("WWW-Authenticate", "Basic realm=dbg")
			writeStatus(w, r, 401)
		}

	}
}

func (a *App) indexHandler(w http.ResponseWriter, r *http.Request) {
	if err := render(w, "config.tmpl", pageData{Name: "index", Config: a.config}); err != nil {
		writeStatus(w, r, 404)
	}
}

func (a *App) objectsHandler(w http.ResponseWriter, r *http.Request) {
	objects, err := a.metaStore.Objects()
	if err != nil {
		fmt.Fprintf(w, "Error retrieving objects: %s", err)
		return
	}

	if err := render(w, "objects.tmpl", pageData{Name: "objects", Objects: objects}); err != nil {
		writeStatus(w, r, 404)
	}
}

func (a *App) objectsRawHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	rv := &RequestVars{Oid: vars["oid"]}

	meta, err := a.metaStore.UnsafeGet(rv)
	if err != nil {
		writeStatus(w, r, 404)
		return
	}

	content, err := a.contentStore.Get(meta, 0)
	if err != nil {
		writeStatus(w, r, 404)
		return
	}
	defer content.Close()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s;", vars["oid"]))
	w.Header().Set("Content-Transfer-Encoding", "binary")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", meta.Size))
	io.Copy(w, content)
}

func (a *App) locksHandler(w http.ResponseWriter, r *http.Request) {
	locks, err := a.metaStore.AllLocks()
	if err != nil {
		fmt.Fprintf(w, "Error retrieving locks: %s", err)
		return
	}

	if err := render(w, "locks.tmpl", pageData{Name: "locks", Locks: locks}); err != nil {
		writeStatus(w, r, 404)
	}
}

func (a *App) usersHandler(w http.ResponseWriter, r *http.Request) {
	users, err := a.metaStore.Users()
	if err != nil {
		fmt.Fprintf(w, "Error retrieving users: %s", err)
		return
	}

	if err := render(w, "users.tmpl", pageData{Name: "users", Users: users}); err != nil {
		writeStatus(w, r, 404)
	}
}

func (a *App) addUserHandler(w http.ResponseWriter, r *http.Request) {
	user := r.FormValue("name")
	pass := r.FormValue("password")
	if user == "" || pass == "" {
		fmt.Fprint(w, "Invalid username or password")
		return
	}

	if err := a.metaStore.AddUser(user, pass); err != nil {
		fmt.Fprintf(w, "Error adding user: %s", err)
		return
	}

	http.Redirect(w, r, "/mgmt/users", http.StatusFound)
}

func (a *App) delUserHandler(w http.ResponseWriter, r *http.Request) {
	user := r.FormValue("name")
	if user == "" {
		fmt.Fprint(w, "Invalid username")
		return
	}

	if err := a.metaStore.DeleteUser(user); err != nil {
		fmt.Fprintf(w, "Error deleting user: %s", err)
		return
	}

	http.Redirect(w, r, "/mgmt/users", http.StatusFound)
}

func render(w http.ResponseWriter, tmpl string, data pageData) error {
	bodyString, err := templateBox.String("body.tmpl")
	if err != nil {
		return err
	}

	contentString, err := templateBox.String(tmpl)
	if err != nil {
		return err
	}

	t := template.Must(template.New("main").Parse(bodyString))
	t.New("content").Parse(contentString)

	return t.Execute(w, data)
}
