ALTER TABLE chairs ADD INDEX idx_access_token (access_token);

alter table rides add index idx_chair_id(chair_id);
alter table ride_statuses add index idx_ride_id(ride_id);
alter table ride_statuses add index idx_ride_id_and_app_sent_at(ride_id, app_sent_at);
alter table chair_locations add index idx_chair_id(chair_id);
alter table rides add index idx_user_id(user_id);
alter table coupons add index idx_used_by(used_by);
