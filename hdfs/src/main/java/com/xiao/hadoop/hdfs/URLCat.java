package com.xiao.hadoop.hdfs;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * Hadoop URL中读取hdfs数据
 *
 * @author 肖亭
 * @since 2018年01月17 11:57
 **/
public class URLCat {

    static {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void main(String[] args) {
        InputStream in = null;
        try {
            in = new URL(args[0]).openStream();
            IOUtils.copyBytes(in, System.out, 1024, false);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
