package com.xiao.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * 使用FileSystem api读取数据
 * 查询
 *
 * @author 肖亭
 * @since 2018年01月17 22:53
 **/
public class FileSystemCat {


    public static void main(String[] args) {
        String uri = args[0];
        getLocal();
        Configuration configuration = new Configuration();
        try {
            System.out.println("uri--start");
            FileSystem fs = FileSystem.get(URI.create(uri), configuration);
            InputStream open = fs.open(new Path(uri));
            IOUtils.copyBytes(open, System.out, 1204, false);
            System.out.println("uri--end");
        } catch (IOException e) {
            e.printStackTrace();
        }
        seek(uri);
        getPos(uri);
    }

    /**
     * 获取本地的文件系统
     */
    private static void getLocal() {
        Configuration cfg = new Configuration();
        try {
            LocalFileSystem local = FileSystem.getLocal(cfg);
            InputStream open = local.open(new Path(local.getUri() + "user/root/quangle.txt"));
            System.out.println("------使用local获取文件数据 ---------");
            IOUtils.copyBytes(open, System.out, 1204, false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * seek 移动
     * seek可以移动到文件的任何一个绝对位置
     * 使用之后会定位到指定的位置开始操作
     *
     * @param uri
     */
    private static void seek(String uri) {
        System.out.println("FileSystemCat.seek--start");
        Configuration cfg = new Configuration();
        try {
            FileSystem fs = FileSystem.get(URI.create(uri), cfg);
            FSDataInputStream in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 1024, false);
            in.seek(1);
            IOUtils.copyBytes(in, System.out, 1024, false);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("FileSystemCat.seek--end");
    }

    /**
     * 查询 当前相对于起始距离的位置
     *
     * @param uri
     */
    private static void getPos(String uri) {
        System.out.println("FileSystemCat.getPos:--start");
        Configuration cfg = new Configuration();
        try {
            FileSystem fs = FileSystem.get(URI.create(uri), cfg);
            FSDataInputStream in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 1024, false);
            long pos = in.getPos();
            System.out.println("FileSystemCat.getPos:" + pos);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("FileSystemCat.getPos:--end");
    }


 /*   private static void positioned(String uri){
        Configuration cfg = new Configuration();
        try {
            FileSystem fs = FileSystem.get(URI.create(uri), cfg);
            FSDataInputStream in = fs.open(new Path(uri));
            fs.cr
            in.read(2,)

        } catch (IOException e) {
            e.printStackTrace();
        }
    }*/

}
