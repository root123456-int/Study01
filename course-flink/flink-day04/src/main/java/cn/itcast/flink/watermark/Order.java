package cn.itcast.flink.watermark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Order {
	private String id;
	private String userId;
	private Integer money;
	private Long orderTime;
}