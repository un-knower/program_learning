package utils.nc;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class ServerFrame extends WindowAdapter implements ActionListener{
	boolean flag=false;
	Socket client;
	JTextArea ta;
	JFrame serverframe;
	JTextField tf;
	PrintWriter pw;
	ServerSocket server;
	final int port = 9999;
	public ServerFrame(){
		try {
			server=new ServerSocket(port);
			this.ServerFrame("服务器端口号"+server.getLocalPort());
			this.serve();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void serve() throws IOException{
		flag=true;
		while(flag){
			client=server.accept();
			System.out.println("fuwu已建立连接！");
			//输入流
			InputStream is=client.getInputStream();
			BufferedReader bri=new BufferedReader(new InputStreamReader(is));
			//输出流
			OutputStream os=client.getOutputStream();
			//参数true表示每写一行，PrintWriter缓存就自动溢出，把数据写到目的地
			pw=new PrintWriter(os,true);
			String str;
			while((str=bri.readLine())!=null){
				ta.append(str+"\n");
				if(str.equals("bye")){
					flag=false;
					break;
				}
			}
			is.close();
			bri.close();
			os.close();
			pw.close();
			
		}
			
	}
	
	public void actionPerformed(ActionEvent e) {
		ta.append(tf.getText()+"\n");
		pw.println(tf.getText());
		tf.setText("");
		
	}
	public void windowClosing(WindowEvent e){
		pw.println("bye");
		System.exit(0);
	}
	
	public void ServerFrame(String port){
		serverframe=new JFrame(""+port);
		serverframe.setLayout(new BorderLayout());
		serverframe.setSize(400,300);
		ta=new JTextArea();
		ta.setEditable(false);
		tf=new JTextField(20);
		JButton send=new JButton("Send");
		send.addActionListener(this);
		tf.addActionListener(this);
		Container container=serverframe.getContentPane();
		container.add(ta,BorderLayout.CENTER);
		JPanel p=new JPanel();
		p.add(tf);
		p.add(send);
		container.add(p,BorderLayout.SOUTH);
		serverframe.addWindowListener(this);
		serverframe.setVisible(true);
	}
	public static void main(String args[]){
		new ServerFrame();
	}
}

