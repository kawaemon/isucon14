# based on https://github.com/oribe1115/isucon13/blob/main/Taskfile.yaml

# change below
# ----------
SERVER_ID = s3

USER = isucon
BIN_NAME = isuride
SRC_DIR = /home/$(USER)/webapp
BUILD_DIR = /home/$(USER)/webapp/go
ENV_FILE = /home/$(USER)/env.sh
SERVICE_NAME = isuride-go.service
# ------------


ROOT_DIR = $(dir $(realpath $(firstword $(MAKEFILE_LIST))))
THIS_SERVER_DIR = $(ROOT_DIR)/$(SERVER_ID)

apply: ghpull setconf
	cd $(BUILD_DIR) && go build -o $(BIN_NAME) .

	sudo systemctl daemon-reload
	sudo systemctl restart mysql nginx
	sudo systemctl restart $(SERVICE_NAME)

ghpull:
	git pull --force


logpermission:
	sudo chmod +rx /var/log/nginx
	sudo chmod +r /var/log/nginx/access.log
	sudo chmod +rx /var/log/mysql
	sudo chmod +r /var/log/mysql/mysql-slow.log


define copy_chown
	mkdir -p $(dir $(THIS_SERVER_DIR)/$(1))
	sudo cp -r $(1) $(THIS_SERVER_DIR)/$(1)
	sudo chown -R $(USER) $(THIS_SERVER_DIR)/$(1)
endef
getconf:
	$(call copy_chown,/etc/systemd/system/$(SERVICE_NAME))
	$(call copy_chown,/etc/nginx)
	$(call copy_chown,/etc/mysql)
	cp $(ENV_FILE) $(THIS_SERVER_DIR)

getsource:
	mv $(SRC_DIR) $(ROOT_DIR)/app


define rm_copy
	sudo rm -rf $(1)
	sudo cp -r $(THIS_SERVER_DIR)/$(1) $(1)
endef
setconf:
	$(call rm_copy,/etc/systemd/system/$(SERVICE_NAME))
	$(call rm_copy,/etc/nginx)
	$(call rm_copy,/etc/mysql)
	cp $(THIS_SERVER_DIR)/$(notdir $(ENV_FILE)) $(ENV_FILE)

linksource:
	ln -sv $(ROOT_DIR)/app $(SRC_DIR)
