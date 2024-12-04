package main

import (
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
)

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
func internalGetMatching(w http.ResponseWriter, r *http.Request) {
	// MEMO: 一旦最も待たせているリクエストに適当な空いている椅子マッチさせる実装とする。おそらくもっといい方法があるはず…
	rides := []*Ride{}
	if err := db.Select(&rides, `SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at`); err != nil {
		if errors.Is(err, sql.ErrNoRows) || len(rides) == 0 {
			slog.Info("no rides for waiting", "err", err)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	slog.Info("rides for waiting", "count", len(rides))

	for _, ride := range rides {
		matched := &Chair{}
		empty := false
		for i := 0; i < 10; i++ {
			if err := db.Get(matched, "SELECT * FROM chairs INNER JOIN (SELECT id FROM chairs WHERE is_active = TRUE ORDER BY RAND() LIMIT 1) AS tmp ON chairs.id = tmp.id LIMIT 1"); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					w.WriteHeader(http.StatusNoContent)
					return
				}
				writeError(w, http.StatusInternalServerError, err)
			}

			if err := db.Get(&empty, "SELECT COUNT(*) = 0 FROM (SELECT COUNT(chair_sent_at) = 6 AS completed FROM ride_statuses WHERE ride_id IN (SELECT id FROM rides WHERE chair_id = ?) GROUP BY ride_id) is_completed WHERE completed = FALSE", matched.ID); err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
			if empty {
				break
			}
		}
		if !empty {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		slog.Info("matched", "ride", ride.ID, "chair", matched.ID)
		if _, err := db.Exec("UPDATE rides SET chair_id = ? WHERE id = ?", matched.ID, ride.ID); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	}

	w.WriteHeader(http.StatusNoContent)
}
