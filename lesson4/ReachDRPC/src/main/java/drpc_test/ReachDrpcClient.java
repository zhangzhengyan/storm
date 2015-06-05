package drpc_test;

import backtype.storm.utils.DRPCClient;

public class ReachDrpcClient
{
    public static void main(String[] args) throws Exception
    {
        DRPCClient client = new DRPCClient("172.16.1.31", 3772);
        System.out.println(client.execute("reach", args[0]));
    }
}
