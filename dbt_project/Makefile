# --- CẤU HÌNH BIẾN ---
DOCKER_USER := lamtran911
IMAGE_NAME := dbt-crypto-pipeline

# 1. Tự động lấy 7 ký tự đầu của Git commit hiện tại (VD: a1b2c3d)
# Nếu không tìm thấy Git (chưa init), mặc định sẽ dùng chữ 'latest'
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo latest)

# 2. Đặt Tag chính là mã Commit ID vừa lấy được
TAG := $(GIT_COMMIT)

# 3. Tên đầy đủ của các image
FULL_IMAGE_NAME := $(DOCKER_USER)/$(IMAGE_NAME):$(TAG)
LATEST_IMAGE_NAME := $(DOCKER_USER)/$(IMAGE_NAME):latest

# --- CÁC LỆNH (TARGETS) ---

.PHONY: help build push all run clean

help:
	@echo "Quản lý Docker Image cho dbt (Chuẩn CI/CD)"
	@echo "------------------------------------------"
	@echo "Mã Commit hiện tại: $(TAG)"
	@echo "------------------------------------------"
	@echo "Lệnh khả dụng:"
	@echo "  make build  - Build Docker image (Gắn tag: $(TAG) và latest)"
	@echo "  make push   - Đẩy cả 2 tag lên Docker Hub"
	@echo "  make all    - Build và đẩy image (Gộp 2 bước)"
	@echo "  make run    - Chạy thử image phiên bản $(TAG) ở local"
	@echo "  make clean  - Xóa image khỏi máy local"

build:
	@echo "Đang build image với tag: $(TAG)"
	# Gắn cùng lúc 2 tag cho image
	docker build -t $(FULL_IMAGE_NAME) -t $(LATEST_IMAGE_NAME) .

push:
	@echo "Đang đẩy image $(FULL_IMAGE_NAME) và $(LATEST_IMAGE_NAME) lên Docker Hub..."
	docker push $(FULL_IMAGE_NAME)
	docker push $(LATEST_IMAGE_NAME)

all: build push
	@echo "Đã hoàn tất build và push phiên bản $(TAG)"

run:
	@echo "Đang chạy thử dbt image phiên bản $(TAG)..."
	docker run --rm $(FULL_IMAGE_NAME) --version

clean:
	@echo "Đang xóa image $(TAG) và latest ở local..."
	# Thêm || true để không báo lỗi nếu image không tồn tại
	docker rmi $(FULL_IMAGE_NAME) $(LATEST_IMAGE_NAME) || true