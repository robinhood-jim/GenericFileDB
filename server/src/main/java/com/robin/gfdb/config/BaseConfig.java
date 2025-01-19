package com.robin.gfdb.config;

import com.robin.core.base.dao.JdbcDao;
import com.robin.core.query.util.QueryFactory;
import com.robin.core.sql.util.BaseSqlGen;
import com.robin.core.sql.util.MysqlSqlGen;
import com.robin.msf.dao.GenericJdbcDao;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Bean;

import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Value;
import io.micronaut.inject.qualifiers.Qualifiers;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobHandler;

import javax.sql.DataSource;

public class BaseConfig {
    @Value("${project.queryconfigpath}")
    private String queryConfigPath;

    @Inject
    private ApplicationContext applicationContext;
    @Singleton
    @Bean
    @Named("queryFactory")
    public QueryFactory getFactory() {
        QueryFactory factory=new QueryFactory();
        factory.setXmlConfigPath(queryConfigPath);
        factory.afterPropertiesSet();
        return factory;
    }
    @Singleton
    @Bean
    @Named("lobHandler")
    public LobHandler getLobHandler(){
        return new DefaultLobHandler();
    }
    @Singleton
    @Bean
    @Named("sqlGen")
    public BaseSqlGen getSqlGen(){
        return MysqlSqlGen.getInstance();
    }
    @Singleton
    @Bean
    @Named("jdbcDao")
    @Requires(beans ={QueryFactory.class,DataSource.class,LobHandler.class,BaseSqlGen.class})
    public GenericJdbcDao getJdbc(@Named("sqlGen") BaseSqlGen sqlGen, @Named("queryFactory") QueryFactory queryFactory, @Named("lobHandler") LobHandler lobHandler){
        return new GenericJdbcDao(applicationContext,null,sqlGen,queryFactory,lobHandler);
    }

    @Singleton
    @Bean
    @Named("trans")
    public DataSourceTransactionManager getTranscationTrans(){
        return new DataSourceTransactionManager(applicationContext.getBean(DataSource.class, Qualifiers.byName("source1")));
    }
}
