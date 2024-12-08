package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

func removeIndex[T any](slice []T, index int) []T {
	if index < 0 || index >= len(slice) {
		return slice
	}
	return append(slice[:index], slice[index+1:]...)
}

func doMatching(ctx context.Context) {
	// MEMO: 一旦最も待たせているリクエストに適当な空いている椅子マッチさせる実装とする。おそらくもっといい方法があるはず…
	rides := []Ride{}
	if err := db.SelectContext(ctx, &rides, `SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at`); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			slog.Warn("no matching candidates")
			return
		}
		slog.Error("error finding matching candidates", err)
		return
	}
	rides_count := len(rides)

	active_chairs := []Chair{}
	if err := db.SelectContext(ctx, &active_chairs, `select * from chairs where is_active = true`); err != nil {
		slog.Error("error finding active chairs", err)
		return
	}
	active_chairs_count := len(active_chairs)

	ok_chairs := []Chair{}
	for _, act := range active_chairs {
		fine := false
		if err := db.GetContext(ctx, &fine, "SELECT COUNT(*) = 0 FROM (SELECT COUNT(chair_sent_at) = 6 AS completed FROM ride_statuses WHERE ride_id IN (SELECT id FROM rides WHERE chair_id = ?) GROUP BY ride_id) is_completed WHERE completed = FALSE", act.ID); err != nil {
			slog.Error("500 3", err)
			return
		}

		if fine {
			ok_chairs = append(ok_chairs, act)
		}
	}
	ok_chairs_count := len(ok_chairs)

	type pair struct {
		rideID  string
		chairID string
		dist    int
	}
	pairs := []pair{}

	for _, ride := range rides {
		best_index := -1
		best_dist := -1
		for i, v := range ok_chairs {
			cache, ok := chairPositionCache.Get(v.ID)
			if !ok && best_index == -1 {
				best_index = i
				continue
			}

			dist := absDiffInt(ride.PickupLatitude, cache.LastLat) + absDiffInt(
				ride.PickupLongitude, cache.LastLong,
			)

			if best_dist == -1 || best_dist < dist {
				best_index = i
				best_dist = dist
				continue
			}
		}

		if best_index != -1 {
			pairs = append(pairs, pair{chairID: ok_chairs[best_index].ID, rideID: ride.ID, dist: best_dist})
			ok_chairs = removeIndex(ok_chairs, best_index)
		}
	}

	for _, v := range pairs {
		if _, err := db.ExecContext(ctx, "UPDATE rides SET chair_id = ? WHERE id = ?", v.chairID, v.rideID); err != nil {
			slog.Error("failed to assign", err)
			return
		}
	}

	slog.Info(
		fmt.Sprintf(
			"### MATCHING ###; want=%d, act=%d, ok=%d, matched=%d", rides_count, active_chairs_count, ok_chairs_count, len(pairs),
		),
	)
}

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
func spwanMatchingProcess() {
	ticker := time.NewTicker(100 * time.Millisecond)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				doMatching(context.Background())
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
}
