package com.huginn.DB.client;

import java.util.Scanner;

/**
 * 客户端有一个简单的 Shell，实际上只是读入用户的输入，并调用 Client.execute()。
 */
public class Shell {
    private Client client;

    public Shell(Client client) {
        this.client = client;
    }

    /**
     * 不断地读入用户的输入，并调用client去执行相应的命令
     */
    public void run(){
        Scanner sc = new Scanner(System.in);

        try {
            while(true){
                System.out.println(":> ");
                String statStr = sc.nextLine();
                if("exit".equals(statStr) || "quit".equals(statStr)){  //当用户输入exit和quit则退出
                    break;
                }

                try {
                    byte[] res = client.execute(statStr.getBytes());        //调用client执行相应的命令然后输出执行结果
                    System.out.println(new String(res));
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        } finally {
            sc.close();
            client.close();
        }

    }
}
