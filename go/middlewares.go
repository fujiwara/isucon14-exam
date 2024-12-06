package main

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"sync"
)

var sessionCache = sync.Map{}

func appAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := r.Cookie("app_session")
		if errors.Is(err, http.ErrNoCookie) || c.Value == "" {
			writeError(w, http.StatusUnauthorized, errors.New("app_session cookie is required"))
			return
		}
		accessToken := c.Value
		if u, ok := sessionCache.Load(accessToken); ok {
			ctx := context.WithValue(r.Context(), "user", u.(*User))
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		user := &User{}
		err = db.Get(user, "SELECT * FROM users WHERE access_token = ?", accessToken)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				writeError(w, http.StatusUnauthorized, errors.New("invalid access token"))
				return
			}
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		sessionCache.Store(accessToken, user)

		ctx := context.WithValue(r.Context(), "user", user)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

var ownerSessionCache = sync.Map{}

func ownerAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := r.Cookie("owner_session")
		if errors.Is(err, http.ErrNoCookie) || c.Value == "" {
			writeError(w, http.StatusUnauthorized, errors.New("owner_session cookie is required"))
			return
		}
		accessToken := c.Value
		if u, ok := ownerSessionCache.Load(accessToken); ok {
			ctx := context.WithValue(r.Context(), "owner", u.(*Owner))
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		owner := &Owner{}
		if err := db2.Get(owner, "SELECT * FROM owners WHERE access_token = ?", accessToken); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				writeError(w, http.StatusUnauthorized, errors.New("invalid access token"))
				return
			}
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		ownerSessionCache.Store(accessToken, owner)

		ctx := context.WithValue(r.Context(), "owner", owner)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

var chairSessionCache = sync.Map{}

func chairAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := r.Cookie("chair_session")
		if errors.Is(err, http.ErrNoCookie) || c.Value == "" {
			writeError(w, http.StatusUnauthorized, errors.New("chair_session cookie is required"))
			return
		}
		accessToken := c.Value

		chair := &Chair{}
		var id string
		if _id, ok := chairSessionCache.Load(accessToken); ok {
			id = _id.(string)
			err = db.Get(chair, "SELECT * FROM chairs WHERE id = ?", id)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					writeError(w, http.StatusUnauthorized, errors.New("invalid access token"))
					return
				}
				writeError(w, http.StatusInternalServerError, err)
				return
			}
		} else {
			err = db.Get(chair, "SELECT * FROM chairs WHERE access_token = ?", accessToken)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					writeError(w, http.StatusUnauthorized, errors.New("invalid access token"))
					return
				}
				writeError(w, http.StatusInternalServerError, err)
				return
			}
			chairSessionCache.Store(accessToken, chair.ID)
		}

		ctx := context.WithValue(r.Context(), "chair", chair)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
