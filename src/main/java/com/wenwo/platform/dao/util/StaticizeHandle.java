package com.wenwo.platform.dao.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSFile;

public class StaticizeHandle {
	private static final Logger LOGGER = LoggerFactory.getLogger(StaticizeHandle.class);	
	Mongo connection;
	DB db;
	GridFS myFS;
	String UrlHostPort = "192.168.1.65:8080";
	String mongoDBHost = "192.168.1.60";
	int mongoDBPort = 30000;
	String dbName = "db";
	/**
	 * 初始化
	 */
	public StaticizeHandle(){
		try {
			connection = new MongoClient(mongoDBHost,mongoDBPort);
			db = connection.getDB(dbName);
			myFS = new GridFS(db);
		} catch (UnknownHostException e) {
			LOGGER.info("数据库连接失败");
		}
		
	}
	/**
	 * 根据Id返回静态代码文件流
	 * @param QuestionId
	 * @return
	 */
	public  InputStream generator(final String QuestionId,String encoding){
		if(StringUtils.isBlank(QuestionId)){
			return null;
		}
		InputStream input = null;
		try {
			URL url = new URL("http://"+UrlHostPort+"/question.do?act=getQuestionDetail&questionId="+QuestionId);
			URLConnection urlConnection = url.openConnection();
			input = urlConnection.getInputStream();
		} catch (Exception e) {
			LOGGER.info("静态化失败",e);
		}
		return input;
	}
	/**
	 * 保存流到mongo的GridFS
	 * @param input
	 * @param fileName
	 */
	public void saveFileToFS(InputStream input,String id,String fileName){
		GridFSDBFile gridFSDBFile = getById(id);
		if(gridFSDBFile!=null){
			myFS.remove(id);
		}
		GridFSFile fsFile = myFS.createFile(input);
		fsFile.put("_id", id);
		fsFile.put("filename",fileName);
		fsFile.put("uploadDate", new Date());
		fsFile.put("contentType", "text/html");
		fsFile.save();
	}
	/**
	 * 根据Id返回文件，只返回第一个
	 * @param id
	 * @return
	 */
	public GridFSDBFile getById(Object id){
		DBObject query = new BasicDBObject("_id",id);
		GridFSDBFile gridFSDBFile = myFS.findOne(query);
		return gridFSDBFile;
	}
	/**
	 * 根据文件名返回文件，只返回第一个
	 * @param fileName
	 * @return
	 */
	public InputStream getByFileName(String fileName){
		DBObject query = new BasicDBObject("filename", fileName);
		GridFSDBFile gridFSDBFile = myFS.findOne(query);
		return gridFSDBFile.getInputStream();
	}
	
	public static void main(String[] args) throws FileNotFoundException {
		String id = "5130025be4b02ec5497d2ae5";
		String fileName = id+".htm";
		StaticizeHandle util = new StaticizeHandle();
		InputStream is = util.generator(id, "UTF-8");
		if(is!=null){
			util.saveFileToFS(is, id, fileName);
			InputStream i = util.getByFileName(fileName);
			if(i!=null){
				BufferedReader in = new BufferedReader(new InputStreamReader(i));				
				try {
					String line;
					while((line=in.readLine())!=null){
						System.out.println(in.readLine());
					}
				} catch (IOException e) {
					LOGGER.info("IO异常：",e);
				}
			}				
		}
		
	}
}
