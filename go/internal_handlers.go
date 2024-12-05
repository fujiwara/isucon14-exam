package main

import (
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"sort"
	"time"

	"github.com/samber/lo"
)

type matching struct {
	Ride  *Ride
	Chair *Chair
	Score float64
	PD    int
	DD    int
	Age   float64
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

	//chairとrideのマッチングをするためにスコアを計算
	matchings := []matching{}
	for _, chair := range chairs {
		for _, ride := range rides {
			pickupDistance := calculateDistance(*chair.Latitude, *chair.Longitude, ride.PickupLatitude, ride.PickupLongitude)
			destinationDistance := calculateDistance(ride.PickupLatitude, ride.PickupLongitude, ride.DestinationLatitude, ride.DestinationLongitude)
			age := time.Since(ride.CreatedAt).Seconds()
			var score float64
			// pickupDistanceは少ないほどよい
			if pickupDistance == 0 {
				score += 25
			} else {
				score += 25 / float64(pickupDistance)
			}
			// destinationDistanceは多いほどよい
			score += float64(destinationDistance) / 10
			// ageは少ないほどよい
			score += 10 / age
			matchings = append(matchings, matching{
				Ride: ride, Chair: chair, Score: score,
				PD: pickupDistance, DD: destinationDistance, Age: age,
			})
		}
	}
	// スコアが高い順に並び替え
	sort.SliceStable(matchings, func(i, j int) bool {
		return matchings[i].Score > matchings[j].Score
	})
	matchedRides := map[string]bool{}
	matchedChairs := map[string]bool{}
	comletedMatchings := []matching{}

	for _, m := range matchings {
		if matchedRides[m.Ride.ID] || matchedChairs[m.Chair.ID] {
			continue
		}
		if m.PD > 100 {
			// 遠すぎる
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

	for _, chunk := range lo.Chunk(comletedMatchings, 20) {
		notifies := map[string]notify{}
		tx, err := db.Beginx()
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		for _, m := range chunk {
			slog.Info("matched", "score", m.Score, "pd", m.PD, "dd", m.DD, "age", m.Age, "ride_id", m.Ride.ID, "chair_id", m.Chair.ID)
			if _, err := db.Exec("UPDATE rides SET chair_id = ? WHERE id = ?", m.Chair.ID, m.Ride.ID); err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
			notifies[m.Chair.ID] = notify{Ride: m.Ride, Status: "MATCHING"}
		}
		tx.Commit()
		for chairID, ns := range notifies {
			sendNotificationSSE(chairID, ns.Ride, ns.Status)
		}
	}
	slog.Info("matched", "count", len(comletedMatchings))
	w.WriteHeader(http.StatusNoContent)
}
