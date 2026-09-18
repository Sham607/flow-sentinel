.PHONY: all build release test check run start stop restart status logs clean tui

all: build

build:
	cargo build

release:
	cargo build --release

test:
	cargo test

check:
	cargo check

run:
	cargo run --release

tui:
	cargo run --release --bin sentinel-tui

start:
	systemctl --user start flow-sentinel.service
	@echo "flow-sentinel service started."
	@systemctl --user status flow-sentinel.service --no-pager

stop:
	systemctl --user stop flow-sentinel.service
	@echo "flow-sentinel service stopped."

restart:
	systemctl --user restart flow-sentinel.service
	@echo "flow-sentinel service restarted."
	@systemctl --user status flow-sentinel.service --no-pager

status:
	systemctl --user status flow-sentinel.service --no-pager

logs:
	journalctl --user -u flow-sentinel.service -f

clean:
	cargo clean
