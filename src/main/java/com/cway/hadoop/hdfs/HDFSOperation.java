package com.cway.hadoop.hdfs;

import com.sun.xml.bind.v2.schemagen.xmlschema.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * 1、查看文件
 * 2、创建新文件夹
 * 3、上传文件
 * 4、下载文件
 * 5、删除文件
 * 6、内部移动
 * 7、内部复制
 * 8、重命名
 * 9、创建新的文件
 * 10、写文件
 * 11、读文件内容
 */
public class HDFSOperation {
    public static void main(String[] args) throws IOException {
        //操作HDFS之前得先创建配置对象
        Configuration conf = new Configuration(true);
        //创建操作HDFS的对象
        FileSystem fs = FileSystem.get(conf);

        //查看文件系统的内容
//		List list = listFileSystem(fs,"/");
        //创建文件夹
//		createDir(fs,"/test/abc");

        //上传文件
//		uploadFileToHDFS(fs,"d:/wc","/test/abc/");

        //下载文件
//		downLoadFileFromHDFS(fs,"/test/abc/wc","d:/");

        //删除.....

        //重命名
//		renameFile(fs,"/test/abc/wc","/test/abc/Angelababy");

        //内部移动 内部复制
//		innerCopyAndMoveFile(fs,conf,"/test/abc/Angelababy","/");

        //创建一个新文件
//		createNewFile(fs,"/test/abc/hanhong");

        //写文件
//		writeToHDFSFile(fs,"/test/abc/hanhong","hello world");
        //追加写
//		appendToHDFSFile(fs,"/test/abc/hanhong","\nhello world");

        //读文件内容
//		readFromHDFSFile(fs,"/test/abc/hanhong");

        //获取数据的位置
        getFileLocation(fs, "/install.log");
    }

    private static void getFileLocation(FileSystem fs, String string) throws IOException {
        FileStatus fileStatus = fs.getFileStatus(new Path(string));
        long len = fileStatus.getLen();
        BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatus, 0, len);
        String[] hosts = fileBlockLocations[0].getHosts();
        for (String string2 : hosts) {
            System.out.println(string2);
        }

        HdfsBlockLocation blockLocation = (HdfsBlockLocation) fileBlockLocations[0];
        long blockId = blockLocation.getLocatedBlock().getBlock().getBlockId();
        System.out.println(blockId);
    }

    private static void readFromHDFSFile(FileSystem fs, String string) throws IllegalArgumentException, IOException {
        FSDataInputStream inputStream = fs.open(new Path(string));

        FileStatus fileStatus = fs.getFileStatus(new Path(string));


        long len = fileStatus.getLen();

        byte[] b = new byte[(int) len];
        int read = inputStream.read(b);
        while (read != -1) {
            System.out.println(new String(b));
            read = inputStream.read(b);
        }


    }

    private static void appendToHDFSFile(FileSystem fs, String filePath, String content) throws IllegalArgumentException, IOException {
        FSDataOutputStream append = fs.append(new Path(filePath));
        append.write(content.getBytes("UTF-8"));
        append.flush();
        append.close();
    }

    private static void writeToHDFSFile(FileSystem fs, String filePath, String content) throws IllegalArgumentException, IOException {
        FSDataOutputStream outputStream = fs.create(new Path(filePath));
        outputStream.write(content.getBytes("UTF-8"));
        outputStream.flush();
        outputStream.close();
    }

    private static void createNewFile(FileSystem fs, String string) throws IllegalArgumentException, IOException {
        fs.createNewFile(new Path(string));
    }

    private static void innerCopyAndMoveFile(FileSystem fs, Configuration conf, String src, String dest) throws IOException {
        Path srcPath = new Path(src);
        Path destPath = new Path(dest);

        //内部拷贝
//		FileUtil.copy(srcPath.getFileSystem(conf), srcPath, destPath.getFileSystem(conf), destPath,false, conf);
        //内部移动
        FileUtil.copy(srcPath.getFileSystem(conf), srcPath, destPath.getFileSystem(conf), destPath, true, conf);
    }

    private static void renameFile(FileSystem fs, String src, String dest) throws IOException {
        Path srcPath = new Path(src);
        Path destPath = new Path(dest);

        fs.rename(srcPath, destPath);

    }

    private static void downLoadFileFromHDFS(FileSystem fs, String src, String dest) throws IOException {
        Path srcPath = new Path(src);
        Path destPath = new Path(dest);
        //copyToLocal
//		fs.copyToLocalFile(srcPath, destPath);
        //moveToLocal
        fs.copyToLocalFile(true, srcPath, destPath);
    }

    private static void uploadFileToHDFS(FileSystem fs, String src, String dest) throws IOException {
        Path srcPath = new Path(src);
        Path destPath = new Path(dest);
        //copyFromLocal
//		fs.copyFromLocalFile(srcPath, destPath);
        //moveFromLocal
        fs.copyFromLocalFile(true, srcPath, destPath);
    }

    private static void createDir(FileSystem fs, String string) throws IllegalArgumentException, IOException {
        Path path = new Path(string);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        fs.mkdirs(path);
    }

    private static List listFileSystem(FileSystem fs, String path) throws FileNotFoundException, IOException {
        Path ppath = new Path(path);

        FileStatus[] listStatus = fs.listStatus(ppath);

        for (FileStatus fileStatus : listStatus) {
            System.out.println(fileStatus.getPath());
        }

        return null;
    }
}
