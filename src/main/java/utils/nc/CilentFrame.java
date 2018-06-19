package utils.nc;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.*;
import java.net.Socket;

public class CilentFrame extends WindowAdapter implements ActionListener{
	boolean flag=false;
	Socket client;
	BufferedReader br;
	PrintWriter pw;
	JFrame cilentframe;
	JTextArea ta;
	JTextField tf;
	public CilentFrame(){
		
		try {client=new Socket("localhost",1234);
			System.out.println("已建立连接！");
			this.CilentFrame("客户端端口号：1234");
			this.talk();
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	public void talk() throws IOException{
		flag=true;
		while(flag){
			InputStream is=client.getInputStream();
			br=new BufferedReader(new InputStreamReader(is));
			
			OutputStream os=client.getOutputStream();
			pw=new PrintWriter(os,true);
			String str;
			while((str=br.readLine())!=null){
				ta.append(str+"\n");
				if(str.equals("bye")){
					flag=false;
					break;
				}
			}
			is.close();
			br.close();
			pw.close();
			System.exit(0);
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
	
	public void CilentFrame(String port){
		cilentframe=new JFrame(""+port);
		cilentframe.setLayout(new BorderLayout());
		cilentframe.setSize(400,300);
		ta=new JTextArea();
		ta.setEditable(false);
		tf=new JTextField(20);
		JButton send=new JButton("Send");
		send.addActionListener(this);
		tf.addActionListener(this);
		Container container=cilentframe.getContentPane();
		container.add(ta,BorderLayout.CENTER);
		JPanel p=new JPanel();
		p.add(tf);
		p.add(send);
		container.add(p,BorderLayout.SOUTH);
		cilentframe.addWindowListener(this);
		cilentframe.setVisible(true);
	}
	public static void main(String args[]){
		new CilentFrame();
	}
}