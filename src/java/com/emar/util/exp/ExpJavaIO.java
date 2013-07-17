/**
 * 提供常见的Java IO 操作实例代码
 * 
 */
package com.emar.util.exp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class ExpJavaIO {

	public ExpJavaIO() {
	}

	static class ReadFromFile {
		/**
		 * 以字节为单位读取文件，常用于读二进制文件，如图片、声音、影像等文件。
		 * 
		 * @param fileName
		 *            文件的名
		 */
		public static void readFileByBytes(String fileName) {
			File file = new File(fileName);
			InputStream in = null;
			try {
				System.out.println("以字节为单位读取文件内容，一次读一个字节：");
				// 一次读一个字节
				in = new FileInputStream(file);
				int tempbyte;
				while ((tempbyte = in.read()) != -1) {
					System.out.write(tempbyte);
				}
				in.close();
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}
			try {
				System.out.println("以字节为单位读取文件内容，一次读多个字节：");
				// 一次读多个字节
				byte[] tempbytes = new byte[100];
				int byteread = 0;
				in = new FileInputStream(fileName);
				ReadFromFile.showAvailableBytes(in);
				// 读入多个字节到字节数组中，byteread为一次读入的字节数
				while ((byteread = in.read(tempbytes)) != -1) {
					System.out.write(tempbytes, 0, byteread);
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			} finally {
				if (in != null) {
					try {
						in.close();
					} catch (IOException e1) {
					}
				}
			}
		}

		/**
		 * 以字符为单位读取文件，常用于读文本，数字等类型的文件
		 * 
		 * @param fileName
		 *            文件名
		 */
		public static void readFileByChars(String fileName) {
			File file = new File(fileName);
			Reader reader = null;
			try {
				System.out.println("以字符为单位读取文件内容，一次读一个字节：");
				// 一次读一个字符
				reader = new InputStreamReader(new FileInputStream(file));
				int tempchar;
				while ((tempchar = reader.read()) != -1) {
					// 对于windows下，rn这两个字符在一起时，表示一个换行。
					// 但如果这两个字符分开显示时，会换两次行。
					// 因此，屏蔽掉r，或者屏蔽n。否则，将会多出很多空行。
					if (((char) tempchar) != 'r') {
						System.out.print((char) tempchar);
					}
				}
				reader.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			try {
				System.out.println("以字符为单位读取文件内容，一次读多个字节：");
				// 一次读多个字符
				char[] tempchars = new char[30];
				int charread = 0;
				reader = new InputStreamReader(new FileInputStream(fileName));
				// 读入多个字符到字符数组中，charread为一次读取字符数
				while ((charread = reader.read(tempchars)) != -1) {
					// 同样屏蔽掉r不显示
					if ((charread == tempchars.length)
							&& (tempchars[tempchars.length - 1] != 'r')) {
						System.out.print(tempchars);
					} else {
						for (int i = 0; i < charread; i++) {
							if (tempchars[i] == 'r') {
								continue;
							} else {
								System.out.print(tempchars[i]);
							}
						}
					}
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			} finally {
				if (reader != null) {
					try {
						reader.close();
					} catch (IOException e1) {
					}
				}
			}
		}

		/**
		 * 以行为单位读取文件，常用于读面向行的格式化文件
		 * 
		 * @param fileName
		 *            文件名
		 */
		public static void readFileByLines(String fileName) {
			File file = new File(fileName);
			BufferedReader reader = null;
			try {
				System.out.println("以行为单位读取文件内容，一次读一整行：");
				reader = new BufferedReader(new FileReader(file));
				String tempString = null;
				int line = 1;
				// 一次读入一行，直到读入null为文件结束
				while ((tempString = reader.readLine()) != null) {
					// 显示行号
					System.out.println("line " + line + ": " + tempString);
					line++;
				}
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (reader != null) {
					try {
						reader.close();
					} catch (IOException e1) {
					}
				}
			}
		}

		/**
		 * 随机读取文件内容
		 * 
		 * @param fileName
		 *            文件名
		 */
		public static void readFileByRandomAccess(String fileName) {
			RandomAccessFile randomFile = null;
			try {
				System.out.println("随机读取一段文件内容：");
				// 打开一个随机访问文件流，按只读方式
				randomFile = new RandomAccessFile(fileName, "r");
				// 文件长度，字节数
				long fileLength = randomFile.length();
				// 读文件的起始位置
				int beginIndex = (fileLength > 4) ? 4 : 0;
				// 将读文件的开始位置移到beginIndex位置。
				randomFile.seek(beginIndex);
				byte[] bytes = new byte[10];
				int byteread = 0;
				// 一次读10个字节，如果文件内容不足10个字节，则读剩下的字节。
				// 将一次读取的字节数赋给byteread
				while ((byteread = randomFile.read(bytes)) != -1) {
					System.out.write(bytes, 0, byteread);
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (randomFile != null) {
					try {
						randomFile.close();
					} catch (IOException e1) {
					}
				}
			}
		}

		/**
		 * 显示输入流中还剩的字节数
		 * 
		 * @param in
		 */
		private static void showAvailableBytes(InputStream in) {
			try {
				System.out.println("当前字节输入流中的字节数为:" + in.available());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public static void main(String[] args) {
			String fileName = "C:/temp/newTemp.txt";
			ReadFromFile.readFileByBytes(fileName);
			ReadFromFile.readFileByChars(fileName);
			ReadFromFile.readFileByLines(fileName);
			ReadFromFile.readFileByRandomAccess(fileName);
		}
	}

	// 二、将内容追加到文件尾部
	/**
	 * 将内容追加到文件尾部
	 */
	static class AppendToFile {
		/**
		 * A方法追加文件：使用RandomAccessFile
		 * 
		 * @param fileName
		 *            文件名
		 * @param content
		 *            追加的内容
		 */
		public static void appendMethodA(String fileName,

		String content) {
			try {
				// 打开一个随机访问文件流，按读写方式
				RandomAccessFile randomFile = new RandomAccessFile(fileName,
						"rw");
				// 文件长度，字节数
				long fileLength = randomFile.length();
				// 将写文件指针移到文件尾。
				randomFile.seek(fileLength);
				randomFile.writeBytes(content);
				randomFile.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		/**
		 * B方法追加文件：使用FileWriter
		 * 
		 * @param fileName
		 * @param content
		 */
		public static void appendMethodB(String fileName, String content) {
			try {
				// 打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件
				FileWriter writer = new FileWriter(fileName, true);
				writer.write(content);
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public static void main(String[] args) {
			String fileName = "C:/temp/newTemp.txt";
			String content = "new append!";
			// 按方法A追加文件
			AppendToFile.appendMethodA(fileName, content);
			AppendToFile.appendMethodA(fileName, "append end. n");
			// 显示文件内容
			ReadFromFile.readFileByLines(fileName);
			// 按方法B追加文件
			AppendToFile.appendMethodB(fileName, content);
			AppendToFile.appendMethodB(fileName, "append end. n");
			// 显示文件内容
			ReadFromFile.readFileByLines(fileName);
		}
	}

	public static class DirOperate {
		public DirOperate() {
		}

		/**
		 * 新建目录
		 * 
		 * @param folderPath
		 *            String 如 c:/fqf
		 * @return boolean
		 */
		public static void newFolder(String folderPath) {
			try {
				String filePath = folderPath;
				filePath = filePath.toString();
				java.io.File myFilePath = new java.io.File(filePath);
				if (!myFilePath.exists()) {
					myFilePath.mkdir();
				}
			} catch (Exception e) {
				System.out.println("新建目录操作出错");
				e.printStackTrace();
			}
		}

		/**
		 * 新建文件
		 * 
		 * @param filePathAndName
		 *            String 文件路径及名称 如c:/fqf.txt
		 * @param fileContent
		 *            String 文件内容
		 * @return boolean
		 */
		public static void newFile(String filePathAndName, String fileContent) {

			try {
				String filePath = filePathAndName;
				filePath = filePath.toString();
				File myFilePath = new File(filePath);
				if (!myFilePath.exists()) {
					myFilePath.createNewFile();
				}
				FileWriter resultFile = new FileWriter(myFilePath);
				PrintWriter myFile = new PrintWriter(resultFile);
				String strContent = fileContent;
				myFile.println(strContent);
				resultFile.close();

			} catch (Exception e) {
				System.out.println("新建目录操作出错");
				e.printStackTrace();

			}

		}

		/**
		 * 删除文件
		 * 
		 * @param filePathAndName
		 *            String 文件路径及名称 如c:/fqf.txt
		 * @param fileContent
		 *            String
		 * @return boolean
		 */
		public static void delFile(String filePathAndName) {
			try {
				String filePath = filePathAndName;
				filePath = filePath.toString();
				java.io.File myDelFile = new java.io.File(filePath);
				myDelFile.delete();

			} catch (Exception e) {
				System.out.println("删除文件操作出错");
				e.printStackTrace();

			}

		}

		/**
		 * 删除文件夹
		 * 
		 * @param filePathAndName
		 *            String 文件夹路径及名称 如c:/fqf
		 * @param fileContent
		 *            String
		 * @return boolean
		 */
		public static void delFolder(String folderPath) {
			try {
				delAllFile(folderPath); // 删除完里面所有内容
				String filePath = folderPath;
				filePath = filePath.toString();
				java.io.File myFilePath = new java.io.File(filePath);
				myFilePath.delete(); // 删除空文件夹

			} catch (Exception e) {
				System.out.println("删除文件夹操作出错");
				e.printStackTrace();

			}

		}

		/**
		 * 删除文件夹里面的所有文件
		 * 
		 * @param path
		 *            String 文件夹路径 如 c:/fqf
		 */
		public static void delAllFile(String path) {
			File file = new File(path);
			if (!file.exists()) {
				return;
			}
			if (!file.isDirectory()) {
				return;
			}
			String[] tempList = file.list();
			File temp = null;
			for (int i = 0; i < tempList.length; i++) {
				if (path.endsWith(File.separator)) {
					temp = new File(path + tempList[i]);
				} else {
					temp = new File(path + File.separator + tempList[i]);
				}
				if (temp.isFile()) {
					temp.delete();
				}
				if (temp.isDirectory()) {
					delAllFile(path + "/" + tempList[i]);// 先删除文件夹里面的文件
					delFolder(path + "/" + tempList[i]);// 再删除空文件夹
				}
			}
		}
		
		/**
		 * 按规则过滤目录下的所有文件和文件夹
		 * @param folderPath
		 * @param regular 规则
		 * @return
		 */
		public static List<String> listFolder(String folderPath, String regular, boolean ispos) {
			if(folderPath == null) {
				return null;
			}
			if (!folderPath.endsWith(File.separator)) { 
				folderPath = folderPath + File.separator;
			}
			File folder = new File(folderPath);
			if(!folder.exists() || !folder.isDirectory()) {
				return null;
			}
			Pattern pattern = null;
			if(regular != null) {
			pattern = Pattern.compile(regular);
			}
			ArrayList<String> paths = new ArrayList<String>();
			ArrayList<String> subpaths;
			String[] flist = folder.list();
			File tmp;
			String tpath;
			for(String s: flist) {
				tpath = folderPath + s;
				tmp = new File(tpath);
				if(tmp.isDirectory()) {
					// 递归调用
					subpaths = (ArrayList<String>) DirOperate.listFolder(folderPath + s, regular, ispos);
					if(subpaths != null) {
						paths.addAll(subpaths);
					}
				} else {
					// 对文件执行 正则过滤： 匹配并丢弃 or 不匹配并保留
					if(pattern != null && ((pattern.matcher(tpath).matches() && !ispos) ||
							(!pattern.matcher(tpath).matches() && ispos))){
							continue;  // 反向匹配
					}
					paths.add(tpath);
				}
			}
			return paths;
		}

		/**
		 * 复制单个文件
		 * 
		 * @param oldPath
		 *            String 原文件路径 如：c:/fqf.txt
		 * @param newPath
		 *            String 复制后路径 如：f:/fqf.txt
		 * @return boolean
		 */
		public static void copyFile(String oldPath, String newPath) {
			try {
				int bytesum = 0;
				int byteread = 0;
				File oldfile = new File(oldPath);
				if (oldfile.exists()) { // 文件存在时
					InputStream inStream = new FileInputStream(oldPath); // 读入原文件
					FileOutputStream fs = new FileOutputStream(newPath);
					byte[] buffer = new byte[1444];
					int length;
					while ((byteread = inStream.read(buffer)) != -1) {
						bytesum += byteread; // 字节数 文件大小
						System.out.println(bytesum);
						fs.write(buffer, 0, byteread);
					}
					inStream.close();
				}
			} catch (Exception e) {
				System.out.println("复制单个文件操作出错");
				e.printStackTrace();

			}

		}

		/**
		 * 复制整个文件夹内容
		 * 
		 * @param oldPath
		 *            String 原文件路径 如：c:/fqf
		 * @param newPath
		 *            String 复制后路径 如：f:/fqf/ff
		 * @return boolean
		 */
		public static void copyFolder(String oldPath, String newPath) {

			try {
				(new File(newPath)).mkdirs(); // 如果文件夹不存在 则建立新文件夹
				File a = new File(oldPath);
				String[] file = a.list();
				File temp = null;
				for (int i = 0; i < file.length; i++) {
					if (oldPath.endsWith(File.separator)) {
						temp = new File(oldPath + file[i]);
					} else {
						temp = new File(oldPath + File.separator + file[i]);
					}

					if (temp.isFile()) {
						FileInputStream input = new FileInputStream(temp);
						FileOutputStream output = new FileOutputStream(newPath
								+ "/" + (temp.getName()).toString());
						byte[] b = new byte[1024 * 5];
						int len;
						while ((len = input.read(b)) != -1) {
							output.write(b, 0, len);
						}
						output.flush();
						output.close();
						input.close();
					}
					if (temp.isDirectory()) {// 如果是子文件夹
						copyFolder(oldPath + "/" + file[i], newPath + "/"
								+ file[i]);
					}
				}
			} catch (Exception e) {
				System.out.println("复制整个文件夹内容操作出错");
				e.printStackTrace();

			}

		}

		/**
		 * 移动文件到指定目录
		 * 
		 * @param oldPath
		 * @param newPath
		 */
		public static void moveFile(String oldPath, String newPath) {
			copyFile(oldPath, newPath);
			delFile(oldPath);

		}

		/**
		 * 移动文件夹到指定目录
		 * 
		 * @param oldPath
		 * @param newPath
		 */
		public static void moveFolder(String oldPath, String newPath) {
			copyFolder(oldPath, newPath);
			delFolder(oldPath);

		}
	}

	public static void main(String[] args) 	{
		String path = "D:/Data/MR-codes/lib/commodityclassify/config";
		String preg = ".*map.*";
		List<String> unfil = DirOperate.listFolder(path, null, true);
		List<String> posfil = DirOperate.listFolder(path, preg, true);
		List<String> negfil = DirOperate.listFolder(path, preg, false);
		System.out.println("[Info] file-path-filter-test:" 
				+ "\nunfil:\t" + unfil.size() + "\t" + unfil 
				+ "\nposfilter:\t" + posfil.size() + "\t" + posfil 
				+ "\nnegfil:\t" + negfil.size() + "\t" + negfil);
	}
}