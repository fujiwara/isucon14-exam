package main

import (
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"sort"
	"time"
)

type matching struct {
	Ride     *Ride
	Chair    *Chair
	Distance int
}

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
func internalGetMatching(w http.ResponseWriter, r *http.Request) {
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

	chairs := []*Chair{}
	if err := db.Select(&chairs, `SELECT * FROM chairs WHERE is_active = TRUE AND latitude IS NOT NULL AND NOT EXISTS (
  SELECT 1
  FROM ride_statuses
  WHERE ride_id IN (SELECT id FROM rides WHERE chair_id = chairs.id)
  GROUP BY ride_id
  HAVING COUNT(chair_sent_at) < 6
)`); err != nil {
		if errors.Is(err, sql.ErrNoRows) || len(chairs) == 0 {
			slog.Info("no active chairs", "err", err)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	slog.Info("active chairs", "count", len(chairs))

	//chairとrideのマッチングをするためにまず全部の距離を求めてしまう
	matchings := []matching{}
	for _, chair := range chairs {
		for _, ride := range rides {
			// ここで距離を計算しておく
			distance := calculateDistance(*chair.Latitude, *chair.Longitude, ride.PickupLatitude, ride.PickupLongitude)
			matchings = append(matchings, matching{Ride: ride, Chair: chair, Distance: distance})
		}
	}
	// 距離が近い順に並べる
	sort.SliceStable(matchings, func(i, j int) bool {
		return matchings[i].Distance < matchings[j].Distance
	})
	matchedRides := map[string]bool{}
	matchedChairs := map[string]bool{}
	comletedMatchings := []matching{}

	cutoff := time.Now().Add(-time.Second * 1) // 1秒前
	for _, m := range matchings {
		if matchedRides[m.Ride.ID] || matchedChairs[m.Chair.ID] {
			continue
		}
		if m.Distance > 25 && m.Ride.CreatedAt.After(cutoff) {
			// 25m以上離れていて、かつ1秒以内に作られたライドは一旦スキップ
			continue
		}
		if m.Distance > 100 {
			// 100m以上離れているものはマッチングしない
			continue
		}
		matchedRides[m.Ride.ID] = true
		matchedChairs[m.Chair.ID] = true
		comletedMatchings = append(comletedMatchings, m)
	}
	if len(comletedMatchings) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()
	for _, m := range comletedMatchings {
		slog.Info("matched", "ride", m.Ride.ID, "chair", m.Chair.ID, "distance", m.Distance, "age", time.Since(m.Ride.CreatedAt))
		if _, err := tx.Exec("UPDATE rides SET chair_id = ? WHERE id = ?", m.Chair.ID, m.Ride.ID); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	}
	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	slog.Info("matched", "count", len(comletedMatchings))
	w.WriteHeader(http.StatusNoContent)
}
