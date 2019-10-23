import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import com.google.common.base.Preconditions;

public class MysqlSink extends AbstractSink implements Configurable {
    private Connection connect;
    private Statement stmt;
    private String columnName;
    private String url;
    private String user;
    private String password;
    private String tableName;
    // 在整个sink结束时执行一遍
    @Override
    public synchronized void stop() {
        // TODO Auto-generated method stub
        super.stop();
    }

    // 在整个sink开始时执行一遍
    @Override
    public synchronized void start() {
        // TODO Auto-generated method stub
        super.start();
        try {
			
				connect = DriverManager.getConnection(url, user, password);
				// 连接URL为 jdbc:mysql//服务器地址/数据库名 ，后面的2个参数分别是登陆用户名和密码
				stmt = connect.createStatement();
			
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
				
        }


    }

    // 不断循环调用
    @Override
    public Status process() throws EventDeliveryException {
        // TODO Auto-generated method stub

		if(connect!=null&&stmt!=null){
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        Event event = null;
        txn.begin();
        while (true) {
            event = ch.take();
            if (event != null) {
                break;
            }
        }
        try {
            String body = new String(event.getBody());
            if (body.split(",").length == columnName.split(",").length) {
                String sql = "insert into " + tableName + "(" + columnName + ") values(" + body + ")";
                System.out.println("sql的语句是：：===="+sql);
                stmt.executeUpdate(sql);
                txn.commit();
                return Status.READY;
            } else {
                txn.rollback();
                return null;
            }
        } catch (Throwable th) {
            txn.rollback();

            if (th instanceof Error) {
                throw (Error) th;
            } else {
                throw new EventDeliveryException(th);
            }
        } finally {
            txn.close();
        }
		
		}else{
			  try {
			
				connect = DriverManager.getConnection(url, user, password);
				// 连接URL为 jdbc:mysql//服务器地址/数据库名 ，后面的2个参数分别是登陆用户名和密码
				stmt = connect.createStatement();
			
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
				
        }
		
		 Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        Event event = null;
        txn.begin();
        while (true) {
            event = ch.take();
            if (event != null) {
                break;
            }
        }
        try {
            String body = new String(event.getBody());
            if (body.split(",").length == columnName.split(",").length) {
                String sql = "insert into " + tableName + "(" + columnName + ") values(" + body + ")";
                stmt.executeUpdate(sql);
                txn.commit();
                return Status.READY;
            } else {
                txn.rollback();
                return null;
            }
        } catch (Throwable th) {
            txn.rollback();

            if (th instanceof Error) {
                throw (Error) th;
            } else {
                throw new EventDeliveryException(th);
            }
        } finally {
            txn.close();
        }
			
			
		}

    }

    @Override
    public void configure(Context arg0) {
        columnName = arg0.getString("column_name");
        Preconditions.checkNotNull(columnName, "column_name must be set!!");
        url = arg0.getString("url");
        Preconditions.checkNotNull(url, "url must be set!!");
        user = arg0.getString("user");
        Preconditions.checkNotNull(user, "user must be set!!");
        password = arg0.getString("password");
        Preconditions.checkNotNull(password, "password must be set!!");
        tableName = arg0.getString("tableName");
        Preconditions.checkNotNull(tableName, "tableName must be set!!");
    }

}