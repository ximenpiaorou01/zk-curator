package com.block.zk;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class ZkCuratorApplicationTests {

    private  CuratorFramework client;

    @BeforeEach
    void setUp() {
        //重试策略
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(3000, 10);
        //第一种方式
        /**
         * @param connctionString 连接字符串
         * @param sessionTimeoutsMs 会话超时时间 ms
         * @param connectionTimeoutsMs 连接超时时间 ms
         * @param retryPolicy 重试策略
         */
//        curatorFramework = CuratorFrameworkFactory.newClient("192.168.57.130:2181",
//                60 * 1000,
//                15 * 1000,
//                retryPolicy);
        //第二种方式
         client = CuratorFrameworkFactory.builder()
                .connectString("192.168.57.130:2181")
                .sessionTimeoutMs(60 * 1000)
                .connectionTimeoutMs(15 * 1000)
                .retryPolicy(retryPolicy)
                .namespace("zoostudy")//表示创建节点是默认使用/zoostudy开通比如创建/app1时，会/zoostudy/app1
                .build();
         client.start();
    }

    @AfterEach
    void tearDown() {
        client.close();
    }
    /*************************创建节点*********************************/
    /**
     * 创建持久节点不设置值
     * @throws Exception
     */
    @Test
    void testCreate() throws Exception {
        String path = client.create().forPath("/app1");
        System.out.println(path);
    }

    /**
     * 创建持久节点，并设置值
     * @throws Exception
     */
    @Test
    void testCreate_1() throws Exception {
        String path = client.create().forPath("/app2","haha".getBytes());
        System.out.println(path);
    }

    /**
     * 创建临时节点，并设置值
     * @throws Exception
     */
    @Test
    void testCreate_2() throws Exception {
        String path = client.create().withMode(CreateMode.EPHEMERAL).forPath("/app3","haha".getBytes());
        System.out.println(path);
    }

    /**
     * 创建多节点
     * @throws Exception
     */
    @Test
    void testCreate_3() throws Exception {
        //父节点不存在是不能存在子节点
//        String path = client.create().forPath("/app4/p1");
        //如果父节点不存在就创建父节点
        String path = client.create().creatingParentsIfNeeded().forPath("/app4/p1");
        System.out.println(path);
    }

    /*************************查询节点*********************************/
    /**
     * 获取节点信息
     * @throws Exception
     */
    @Test
    void testGet1() throws Exception {
        byte[] bytes = client.getData().forPath("/app1");
        System.out.println(new String(bytes));
    }

    /**
     * 查询某个节点下的子节点
     * @throws Exception
     */
    @Test
    void testGet2() throws Exception {
        List<String> strings = client.getChildren().forPath("/");//获取namespace的zoostudy下节点
        System.out.println(strings);
    }

    /**
     * 查询某个节点状态信息,等同于命令行：ls -s /zoostudy/app1
     * @throws Exception
     */
    @Test
    void testGet3() throws Exception {
        Stat stat = new Stat();
        client.getData()
                .storingStatIn(stat)//把数据放入stat
                .forPath("/app1");
        System.out.println(stat);
    }

    /*************************修改节点*********************************/
    @Test
    void testSet1() throws Exception {
//        client.setData().forPath("/app1","itcast".getBytes());
        //一般修改需要加版本
        //先查询出版本
        Stat stat = new Stat();
        client.getData().storingStatIn(stat).forPath("/app1");
        int version = stat.getVersion();
        //再修改，防止多线程下数据不一致,如果在查询和修改期间有别人修改，version会变，修改失败
        client.setData().withVersion(version).forPath("/app1");
    }

    /*************************删除节点*********************************/
    /**
     * 删除单个节点
     * 删除有子节点的节点
     * 必须成功的删除
     * 回调
     * @throws Exception
     */
    @Test
    void testDelete1() throws Exception {
        //删除单个节点
//      client.delete().forPath("/app1");

        //删除有子节点的节点
//        client.delete().deletingChildrenIfNeeded().forPath("/app4");

        //必须成功的删除,例如网络抖动导致指令发送不成功，这个可以重试保证删除
//        client.delete().guaranteed().forPath("/app2");

        //回调,删除后调用回调方法
        client.delete().guaranteed().inBackground((curatorFramework, curatorEvent)->{
            System.out.println("我被删除了");
            System.out.println(curatorEvent);
        }).forPath("/app4");
    }

    /*************************监听Watcher*********************************/

    /**
     * 演示NodeCache:给指定一个节点注册监听器
     */
    @Test
    void testNodeCache() throws Exception {
        //1.查看NodeCache对象,不压缩
        NodeCache nodeCache = new NodeCache(client,"/app1",false);
        //2.注册监听
        nodeCache.getListenable().addListener(()->{
            System.out.println("节点变化了");
            byte[] data = nodeCache.getCurrentData().getData();
            System.out.println(new String(data));
        });
        //3.开启监听,如果设置为true.则开启监听时，加载缓冲数据
        nodeCache.start(true);
        while(true){

        }
    }

    /**
     * 演示PathChildrenCache:监听ZNode的子节点(ZNode不监听)
     */
    @Test
    void testPathChildrenCache() throws Exception {
        //1.查看NodeCache对象,不压缩
        PathChildrenCache childrenCache = new PathChildrenCache(client, "/app2", true);
        //2.注册监听
        childrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
                System.out.println("节点变化了");
                System.out.println(pathChildrenCacheEvent);
                PathChildrenCacheEvent.Type type = pathChildrenCacheEvent.getType();
                switch (type){
                    case CHILD_ADDED://子节点添加
                        //todo
                        break;
                    case CHILD_UPDATED://子节点数据修改
                        byte[] data = pathChildrenCacheEvent.getData().getData();
                        break;
                    case CHILD_REMOVED://子节点删除
                        //todo
                        break;
                    default:
                        break;
                }
            }
        });
        //3.开启监听,如果设置为true.则开启监听时，加载缓冲数据
        childrenCache.start();
        while(true){

        }
    }



    /**
     * 演示TreeCache:监听某个节点ZNode和它的子节点
     */
    @Test
    void testTreeCache() throws Exception {
        //1.查看NodeCache对象,不压缩
        TreeCache treeCache = new TreeCache(client, "/app2");
        //2.注册监听
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent event) throws Exception {
                System.out.println("节点变化了");
                TreeCacheEvent.Type type = event.getType();
                switch (type){
                    case NODE_ADDED://子节点添加
                        //todo
                        break;
                    case NODE_UPDATED://子节点数据修改
                        byte[] data = event.getData().getData();
                        break;
                    case NODE_REMOVED://子节点删除
                        //todo
                        break;
                    default:
                        break;
                }
            }
        });
        //3.开启监听
        treeCache.start();
        while (true){

        }
    }



    /*************************分布式锁*********************************/
    /**
     * zookeeper分布式锁原理：
     * 1.客户端去zookeeper服务器的节点下创建临时顺序节点(必须是临时顺序，create -es 节点),
     * 2.客户端获取节点下的所有顺序节点，然后数值最小的节点，表示获取到锁
     * 3.其他节点会开启监听比它自己小的节点，比如NODE1获取锁，NODE2开启监听NODE1，NODE3监听NODE2，NODE4监听NODE3
     * 4.NODE1处理业务结束后，主动删除节点或者发送异常，NODE1会自动释放后，NODE2会收到删除通知通知，然后NODE2获取锁
     * 5.NODE2执行任务，删除节点....
     * @throws Exception
     */
    @Test
    void testDistributeLock() throws Exception {
        InterProcessLock lock = new InterProcessMutex(client,"/lock");
        lock.acquire(3, TimeUnit.SECONDS);

        try {
            //todo buiness
        }finally {
            lock.release();
        }

    }


}
