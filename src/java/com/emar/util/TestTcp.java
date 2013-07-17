package com.emar.util;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;

import com.emar.util.HeadParams;

/**
 * 亿起发业务端 与 推荐组 的请求接口联调测试程序。 
 *
 */
public class TestTcp {
	public static void main(String[] args) {

		InetSocketAddress socketAddress = new InetSocketAddress("172.16.8.132",
				7177);

		SocketChannel channel = null;
		try {
			channel = SocketChannel.open(socketAddress);
			if (!channel.isConnected()) {
				System.out.println("服务停止！");
				return;
			}
			channel.configureBlocking(false); // 使用阻塞或者非阻塞的方式返回结果都一样
			for (int i = 0; i < 10000; i++) {
				HeadParams head = new HeadParams("" + i);
				byte[] buf = head.toString().getBytes();
				// System.out.println(buf.length);
				ByteBuffer bb = ByteBuffer.allocate(buf.length);
				bb.put(buf);
				bb.flip();

				if (channel.isConnected()) {
					System.out.println("发送请求：" + head.toString());
					channel.write(bb);
				} else {
					System.out.println("发送请求失败，服务器连接已经关闭。");
				}

				try {
					Thread.sleep(11);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				ByteBuffer buffer = ByteBuffer.allocate(16);
				if (channel.read(buffer) != -1) {
					buffer.flip();
					if (buffer.limit() > 0) {
						// System.out.println(new String(buffer.array()));
						String sdf = new String(buffer.array());
						if (sdf.contains("len=") && sdf.contains("&")) {
							String[] sdfd = sdf.split("&");
							String[] sddd = sdfd[0].split("=");
							int lengtd = Integer.parseInt(sddd[1]);
							int left = lengtd
									- (sdf.length() - sdf.indexOf('&') - 1);
							ByteBuffer new_buffer = ByteBuffer.allocate(left);
							if (channel.read(new_buffer) != -1) {
								String data = new String(new_buffer.array());
								sdf += data;
							}

							System.out.println("返回消息：" +sdf);
						} else {
							System.out.println("返回消息：" + sdf);
						}
					}
					buffer.clear();
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

			// try {
			// channel.close();
			// } catch (IOException e) {
			// // TODO Auto-generated catch block
			// e.printStackTrace();
			// }
			//

		}

	}

	private byte[] getBytes(char[] chars) {// 将字符转为字节(编码)
		Charset cs = Charset.forName("UTF-8");
		CharBuffer cb = CharBuffer.allocate(chars.length);
		cb.put(chars);
		cb.flip();
		ByteBuffer bb = cs.encode(cb);
		return bb.array();
	}

	private char[] getChars(byte[] bytes) {// 将字节转为字符(解码)
		Charset cs = Charset.forName("UTF-8");
		ByteBuffer bb = ByteBuffer.allocate(bytes.length);
		bb.put(bytes);
		bb.flip();
		CharBuffer cb = cs.decode(bb);

		return cb.array();
	}
}
