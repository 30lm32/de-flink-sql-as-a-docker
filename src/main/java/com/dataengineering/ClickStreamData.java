package com.dataengineering;

public class ClickStreamData {
		private Long date = 0L;
		private Long productId = 0L;
		private String eventName = "";
		private Long userId = 0L;

		public ClickStreamData() {

		}

		public Long getDate() {
			return date;
		}

		public void setDate(Long date) {
			this.date = date;
		}

		public Long getProductId() {
			return productId;
		}

		public void setProductId(Long productId) {
			this.productId = productId;
		}

		public String getEventName() {
			return eventName;
		}

		public void setEventName(String eventName) {
			this.eventName = eventName;
		}

		public Long getUserId() {
			return userId;
		}

		public void setUserId(Long userId) {
			this.userId = userId;
		}

		@Override
		public String toString() {
			return "ClickStream{" +
					"date=" + date +
					", productId=" + productId +
					", eventName='" + eventName + '\'' +
					", userId=" + userId +
					'}';
		}

		public static ClickStreamData of(Long date,
                                         Long productId,
                                         String eventName,
										 Long userId) {

			ClickStreamData data = new ClickStreamData();
			data.setDate(date);
			data.setProductId(productId);
			data.setEventName(eventName);
			data.setUserId(userId);

			return data;
		}
	}