package com.wenwo.platform.dao.chainquery.util;

import java.io.Serializable;

import com.wenwo.platform.dao.base.BaseDao;
import com.wenwo.platform.dao.base.CiDianBaseDao;
import com.wenwo.platform.dao.chainquery.ChainQueryer;
import com.wenwo.platform.dao.chainquery.CiDianChainQueryer;


public class ChainQueryUtil {
	public static <T,PK extends Serializable> CiDianChainQueryer<T,PK> attach( CiDianBaseDao<T,  PK > dao){
		return new CiDianChainQueryer<T,PK>(dao);		
	}
	public static <T,PK extends Serializable> ChainQueryer<T,PK> attach( BaseDao<T,  PK > dao){
		return new ChainQueryer<T,PK>(dao);		
	}
	
}
