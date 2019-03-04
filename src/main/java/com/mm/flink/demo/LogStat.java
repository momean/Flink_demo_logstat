package com.mm.flink.demo;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 日志统计：监控日志，输出日志行数、包含Error字符的日志行数
 * 
 * @author mm
 */
public class LogStat {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		// 检查输入参数，参数形式是： --key value 或 -key value
		final ParameterTool params = ParameterTool.fromArgs(args);
		final String hostname;
		final int port;
		try {
			hostname = params.get("hostname", "localhost");
			port = params.getInt("port");
		} catch (Exception e) {
			System.err.println("请指定 --hostname、--port 参数，hostname 默认为 localhost，而 port 必须指定");
			System.err.println("可以使用 netcat -l <port> 快速开启一个输入服务，并在命令行中快速输入内容");
			return;
		}
		// 设置执行环境
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// 让参数在Web界面中可见
		env.getConfig().setGlobalJobParameters(params);
		// 获取输入数据
		DataStream<String> text = env.socketTextStream(hostname, port, "\n");
		DataStream<LogStatInfo> counts = text.flatMap(new FlatMapFunction<String, LogStatInfo>() {
			@Override
			public void flatMap(String value, Collector<LogStatInfo> out) throws Exception {
				DateTimeFormatter dataFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
				String now = dataFormatter.format(LocalDateTime.now());
				out.collect(new LogStatInfo(now, 1, value.contains("Error") ? 1 : 0));
			}
		}).keyBy("time").timeWindow(Time.seconds(10)).reduce(new ReduceFunction<LogStat.LogStatInfo>() {

			@Override
			public LogStatInfo reduce(LogStatInfo a, LogStatInfo b) throws Exception {
				return new LogStatInfo(a.time, a.total + b.total, a.error + b.error);
			}
		});

		// 输出结果
		if (params.has("output")) {
			counts.writeAsText(params.get("output"));
		} else {
			System.out.println("结果输出到*-taskexecutor-*.out，请使用 --output 指定输出路径");
			counts.print().setParallelism(1);
		}

		// 执行程序
		env.execute("Streaming LogStat");
	}

	public static class LogStatInfo {
		public String time;
		public long error;
		public long total;

		public LogStatInfo() {
		}

		public LogStatInfo(String time, long total, long error) {
			this.time = time;
			this.total = total;
			this.error = error;
		}

		@Override
		public String toString() {
			return time + "\t" + total + "\t" + error;
		}

	}
}