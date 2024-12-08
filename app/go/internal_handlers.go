package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"
)

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

	for _, ride := range rides {
		matched := &Chair{}
		empty := false
		for i := 0; i < 10; i++ {
			if err := db.GetContext(ctx, matched, "SELECT * FROM chairs INNER JOIN (SELECT id FROM chairs WHERE is_active = TRUE ORDER BY RAND() LIMIT 1) AS tmp ON chairs.id = tmp.id LIMIT 1"); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					slog.Warn("why 2")
					return
				}
				slog.Error("500 2", err)
			}

			if err := db.GetContext(ctx, &empty, "SELECT COUNT(*) = 0 FROM (SELECT COUNT(chair_sent_at) = 6 AS completed FROM ride_statuses WHERE ride_id IN (SELECT id FROM rides WHERE chair_id = ?) GROUP BY ride_id) is_completed WHERE completed = FALSE", matched.ID); err != nil {
				slog.Error("500 3", err)
				return
			}
			if empty {
				break
			}
		}
		if !empty {
			slog.Warn("empty 4")
			return
		}

		if _, err := db.ExecContext(ctx, "UPDATE rides SET chair_id = ? WHERE id = ?", matched.ID, ride.ID); err != nil {
			slog.Error("failed to assign", err)
			return
		}
	}

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
