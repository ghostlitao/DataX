package com.alibaba.datax.plugin.reader.mdyhttpreader;

import com.alibaba.fastjson2.JSON;
import okhttp3.MediaType;

import java.util.Map;


public class OkHttpUtils {
    public static final MediaType JSON_TYPE = MediaType.parse("application/json; charset=utf-8");

    public static String post(String url, Map<String, Object> json) throws Exception {
        return post(url, JSON.toJSONString(json), JSON_TYPE);
    }

    public static String post(String url, String json) throws Exception {
        return post(url, json, JSON_TYPE);
    }

    public static String post(String url, String json, MediaType mediaType) throws Exception {
        okhttp3.OkHttpClient client = new okhttp3.OkHttpClient();
        okhttp3.RequestBody body = okhttp3.RequestBody.create(mediaType, json);
        okhttp3.Request request = new okhttp3.Request.Builder()
                .url(url)
                .post(body)
                .build();
        okhttp3.Response response = client.newCall(request).execute();
        return response.body().string();
    }

    public static String get(String url) throws Exception {
        okhttp3.OkHttpClient client = new okhttp3.OkHttpClient();
        okhttp3.Request request = new okhttp3.Request.Builder()
                .url(url)
                .build();
        okhttp3.Response response = client.newCall(request).execute();
        return response.body().string();
    }
}
