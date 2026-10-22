# Refresh private local CSVs. This command never starts a paid search.
VPS ?= root@62.238.36.220
VPS_DIR ?= /opt/ai-video-sampling

.PHONY: sync
sync:
	mkdir -p csv
	ssh $(VPS) 'cd $(VPS_DIR) && docker compose run --rm -T worker export'
	scp $(VPS):$(VPS_DIR)/data/exports/snapshot.zip csv/.snapshot.zip
	python3 -m zipfile -e csv/.snapshot.zip csv/
