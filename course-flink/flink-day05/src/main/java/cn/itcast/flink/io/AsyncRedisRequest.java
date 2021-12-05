package cn.itcast.flink.io;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * 基于Flink中提供异步IO实现异步请求Redis数据库，进行结果返回
 */
public class AsyncRedisRequest extends RichAsyncFunction<String, String> {

	// 定义Jedis实例变量
	private Jedis jedis = null ;

	@Override
	public void open(Configuration parameters) throws Exception {
		GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
		poolConfig.setMaxTotal(8);
		poolConfig.setMinIdle(2);
		poolConfig.setMaxIdle(8);
		// 通过JedisPool连接池，获取Jedis对象
		JedisPool jedisPool = new JedisPool(
			poolConfig, "node1.itcast.cn", 6379, 9000
		);
		// 获取Jedis对象
		jedis = jedisPool.getResource() ;
	}

	@Override
	public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
		// input 表示流中每条数据：1,beijing
		String cityName = input.trim().split(",")[1];

		// 第一步、异步请求Redis，由于Jedis客户端不支持异步，所以操作线程池
		ExecutorService service = Executors.newFixedThreadPool(10);
		Future<String> future = service.submit(
			new Callable<String>() {
				@Override
				public String call() throws Exception {
					return jedis.hget("AsyncReadRedis", cityName);
				}
			}
		);

		// 第二步、进行异步回调处理
		CompletableFuture.supplyAsync(new Supplier<String>() {
			@Override
			public String get() {
				try {
					return future.get();
				} catch (InterruptedException | ExecutionException e) {
					return "-1" ;
				}
			}
		}).thenAccept( (String result) -> {
			resultFuture.complete(Collections.singleton(result));
		});
	}

	@Override
	public void close() throws Exception {
		// 关闭Jedis连接
		if(null != jedis) jedis.close();
	}
}
