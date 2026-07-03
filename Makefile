PY ?= python
CFG ?= config/config.yaml
install: ; pip install -r requirements.txt
phase2: ; $(PY) -m phase2_web.pipeline --config $(CFG)
.PHONY: install phase2
