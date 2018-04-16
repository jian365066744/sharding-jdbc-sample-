package com.ronglian;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import io.shardingjdbc.core.api.ShardingDataSourceFactory;
import io.shardingjdbc.core.api.algorithm.sharding.ListShardingValue;
import io.shardingjdbc.core.api.algorithm.sharding.PreciseShardingValue;
import io.shardingjdbc.core.api.algorithm.sharding.RangeShardingValue;
import io.shardingjdbc.core.api.algorithm.sharding.ShardingValue;
import io.shardingjdbc.core.api.algorithm.sharding.complex.ComplexKeysShardingAlgorithm;
import io.shardingjdbc.core.api.config.ShardingRuleConfiguration;
import io.shardingjdbc.core.api.config.TableRuleConfiguration;
import io.shardingjdbc.core.api.config.strategy.ComplexShardingStrategyConfiguration;
import org.apache.commons.dbcp.BasicDataSource;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <b>description</b>： <br>
 * <b>time</b>：2018-04-16 13:30 <br>
 * <b>author</b>： zj
 */
public class DemoTest {

    @Test
    public void test() throws Exception {
        //1、配置真实数据源
        Map<String, DataSource> dataSourceMap = new HashMap<>();
        String dbname = "sharding_0";
        dataSourceMap.put(dbname, this.createDataSource(dbname));
        dbname = "sharding_1";
        dataSourceMap.put(dbname, this.createDataSource(dbname));

        //2、配置order表分片规则
        TableRuleConfiguration orderTableRuleConfig = new TableRuleConfiguration();
        orderTableRuleConfig.setLogicTable("t_order");
        orderTableRuleConfig.setActualDataNodes("sharding_${0..1}.t_order_${[0,1]}");
        orderTableRuleConfig.setDatabaseShardingStrategyConfig(new ComplexShardingStrategyConfiguration("order_id", ComplexKeysShardingAlgorithmTest3Db.class.getName()));
        orderTableRuleConfig.setTableShardingStrategyConfig(new ComplexShardingStrategyConfiguration("order_id", ComplexKeysShardingAlgorithmTest3Table.class.getName()));

        //3、配置分片规则
        ShardingRuleConfiguration shardingRuleConfig = new ShardingRuleConfiguration();
        shardingRuleConfig.getTableRuleConfigs().add(orderTableRuleConfig);

        //4、获取数据源
        Properties props = getProperties();
        DataSource dataSource = ShardingDataSourceFactory.createDataSource(dataSourceMap, shardingRuleConfig, new ConcurrentHashMap(), props);

        //5、执行本地数据操作
//        String sql = "SELECT a.* FROM t_order a WHERE a.order_id BETWEEN ? AND ?";
        String sql = "SELECT a.* FROM t_order a WHERE a.order_id IN (?,?)";
//        String sql = "SELECT a.* FROM t_order a WHERE a.order_id = ?";
        try (
                Connection conn = dataSource.getConnection();
                PreparedStatement preparedStatement = conn.prepareStatement(sql);
        ) {
            preparedStatement.setInt(1, 1);
            preparedStatement.setInt(2, 2);
            try (ResultSet rs = preparedStatement.executeQuery()) {
                while (rs.next()) {
                    System.out.println(rs.getInt(1));
                    System.out.println(rs.getInt(2));
                }
            }
        }
    }

    public static class ComplexKeysShardingAlgorithmTest3Db implements ComplexKeysShardingAlgorithm {
        @Override
        public Collection<String> doSharding(Collection<String> availableTargetNames, Collection<ShardingValue> shardingValues) {
            ShardingValue shardingValue = shardingValues.iterator().next();
            if (shardingValue instanceof PreciseShardingValue) {
                return this.doEqualSharding(availableTargetNames, (PreciseShardingValue<Integer>) shardingValue);
            } else if (shardingValue instanceof ListShardingValue) {
                return this.doInSharding(availableTargetNames, (ListShardingValue<Integer>) shardingValue);
            } else if (shardingValue instanceof RangeShardingValue) {
                return this.doBetweenSharding(availableTargetNames, (RangeShardingValue<Integer>) shardingValue);
            }
            throw new UnsupportedOperationException();
        }

        private Collection<String> doEqualSharding(Collection<String> availableTargetNames, PreciseShardingValue<Integer> shardingValue) {
            List<String> list = Lists.newArrayList();
            Integer value = shardingValue.getValue();
            for (String target : availableTargetNames) {
                this.targetName(target, value, list);
            }
            return list;
        }

        private void targetName(String target, Integer value, List<String> targets) {
            Integer index = value % 4 / 2;
            if (target.endsWith(index.toString())) {
                targets.add(target);
            }
        }

        private Collection<String> doInSharding(Collection<String> availableTargetNames, ListShardingValue<Integer> shardingValue) {
            List<String> list = Lists.newArrayList();
            Collection<Integer> values = shardingValue.getValues();
            for (Integer value : values) {
                for (String target : availableTargetNames) {
                    this.targetName(target, value, list);
                }

            }
            return list;
        }

        private Collection<String> doBetweenSharding(Collection<String> availableTargetNames, RangeShardingValue<Integer> rangeShardingValue) {
            List<String> list = Lists.newArrayList();
            Range<Integer> range = rangeShardingValue.getValueRange();
            for (Integer value = range.lowerEndpoint(); value <= range.upperEndpoint(); value++) {
                for (String target : availableTargetNames) {
                    this.targetName(target, value, list);
                }
            }
            return list;
        }
    }

    public static class ComplexKeysShardingAlgorithmTest3Table implements ComplexKeysShardingAlgorithm {
        @Override
        public Collection<String> doSharding(Collection<String> availableTargetNames, Collection<ShardingValue> shardingValues) {
            ShardingValue shardingValue = shardingValues.iterator().next();
            if (shardingValue instanceof PreciseShardingValue) {
                return this.doEqualSharding(availableTargetNames, (PreciseShardingValue<Integer>) shardingValue);
            } else if (shardingValue instanceof ListShardingValue) {
                return this.doInSharding(availableTargetNames, (ListShardingValue<Integer>) shardingValue);
            } else if (shardingValue instanceof RangeShardingValue) {
                return this.doBetweenSharding(availableTargetNames, (RangeShardingValue<Integer>) shardingValue);
            }
            throw new UnsupportedOperationException();
        }

        private Collection<String> doEqualSharding(Collection<String> availableTargetNames, PreciseShardingValue<Integer> shardingValue) {
            List<String> list = Lists.newArrayList();
            Integer value = shardingValue.getValue();
            for (String target : availableTargetNames) {
                this.targetName(target, value, list);
            }
            return list;
        }

        private void targetName(String target, Integer value, List<String> targets) {
            Integer index = value % 4 % 2;
            if (target.endsWith(index.toString())) {
                targets.add(target);
            }
        }

        private Collection<String> doInSharding(Collection<String> availableTargetNames, ListShardingValue<Integer> shardingValue) {
            List<String> list = Lists.newArrayList();
            Collection<Integer> values = shardingValue.getValues();
            for (Integer value : values) {
                for (String target : availableTargetNames) {
                    this.targetName(target, value, list);
                }

            }
            return list;
        }

        private Collection<String> doBetweenSharding(Collection<String> availableTargetNames, RangeShardingValue<Integer> rangeShardingValue) {
            List<String> list = Lists.newArrayList();
            Range<Integer> range = rangeShardingValue.getValueRange();
            for (Integer value = range.lowerEndpoint(); value <= range.upperEndpoint(); value++) {
                for (String target : availableTargetNames) {
                    this.targetName(target, value, list);
                }
            }
            return list;
        }
    }


    private Properties getProperties() {
        Properties props = new Properties();
        props.put("sql.show", true);
        return props;
    }

    private BasicDataSource createDataSource(String dbname) {
        BasicDataSource dataSource1 = new BasicDataSource();
        dataSource1.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource1.setUrl("jdbc:mysql://localhost:3306/" + dbname);
        dataSource1.setUsername("root");
        dataSource1.setPassword("root");
        return dataSource1;
    }
}
