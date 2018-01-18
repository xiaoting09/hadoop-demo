package com.xiao.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;

/**
 * 将本地文件通过api复制到Hadoop
 * 写入数据
 * @author 肖亭
 * @since 2018年01月17 23:43
 **/
public class FileCopyWithProgress {

    public static void main(String[] args) {
        String localSrc = args[0];
        String dst = args[1];
        try {
            InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
            Configuration cfg = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(dst), cfg);
            FSDataOutputStream stream = fs.create(new Path(dst), () -> System.out.println("........."));
            IOUtils.copyBytes(in, stream, 4096, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        append(dst);
    }

    private static void append(String uri) {
        Configuration cfg = new Configuration();
        try {
            FileSystem fs = FileSystem.get(URI.create(uri), cfg);
            FSDataInputStream in = fs.open(new Path(uri));
            FSDataOutputStream append = fs.append(new Path(uri));
            IOUtils.copyBytes(in, append, 1024, false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
