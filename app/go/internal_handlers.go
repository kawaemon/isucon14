package main

import (
	"database/sql"
	"errors"
	"net/http"
)

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
func internalGetMatching(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	// MEMO: 一旦最も待たせているリクエストに適当な空いている椅子マッチさせる実装とする。おそらくもっといい方法があるはず…
	rides := []Ride{}
	if err := db.SelectContext(ctx, &rides, `SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at`); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	for _, ride := range rides {
		matched := &Chair{}
		if err := db.GetContext(
			ctx,
			matched,
			`
			WITH valid_chars AS (
				SELECT chairs.id
				FROM chairs
				WHERE NOT EXISTS (
					SELECT 1
					FROM (
						SELECT COUNT(chair_sent_at) = 6 AS completed
						FROM ride_statuses
						WHERE ride_id IN (
							SELECT id FROM rides WHERE chair_id = chairs.id
						)
						GROUP BY ride_id
					) is_completed
					WHERE completed = FALSE
				)
			)
			SELECT chairs.*, tmp.distance
			FROM chairs
			INNER JOIN (
					SELECT
							chair_id,
							MAX((ABS(latitude - ?) + ABS(longitude - ?))) AS distance
					FROM chair_locations
					GROUP BY chair_id
			) cl ON chairs.id = cl.chair_id
			INNER JOIN (
					SELECT *
					FROM rides
					INNER JOIN (
							SELECT
									ride_id,
									status,
									MAX(chair_sent_at)
							FROM ride_statuses
							WHERE status = 'COMPLETED'
							GROUP BY ride_id
					) rs ON rides.id = rs.ride_id
			) r ON chairs.id = r.chair_id
			WHERE is_active = TRUE
			ORDER BY distance
			LIMIT 1
		`,
			ride.PickupLatitude,
			ride.PickupLongitude,
		); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			writeError(w, http.StatusInternalServerError, err)
		}
		if matched.ID == "" {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		if _, err := db.ExecContext(ctx, "UPDATE rides SET chair_id = ? WHERE id = ?", matched.ID, ride.ID); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	}

	w.WriteHeader(http.StatusNoContent)
}
