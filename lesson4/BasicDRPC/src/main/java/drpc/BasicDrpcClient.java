package drpc;

import backtype.storm.utils.DRPCClient;

public class BasicDrpcClient
{
    public static void main(String[] args) throws Exception
    {
        DRPCClient client = new DRPCClient("172.16.1.31", 3772);

        for(String word : new String[]{"nihao", "hello", "polly"})
        {
            System.out.println(client.execute("exclamation", word));
        }
    }
}
