using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Serilog;

namespace AppConsole
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var logger = new LoggerConfiguration()
              .WriteTo.Console()
              .CreateLogger();

            if (args.Length > 3 && args[0] == "1")
                await Producer(args);
            else if ( args.Length > 2 && args[0] == "2")
                await Consumer(args);
            else
                logger.Information("Informe os parametros da console");
        }

        static async Task Producer(string[] args)
        {
            var logger = new LoggerConfiguration()
                .WriteTo.Console()
                .CreateLogger();
            logger.Information("Testando o envio de mensagens com Kafka");

            if (args.Length < 3)
            {
                logger.Error(
                    "Informe ao menos 3 parâmetros: " +
                    "no primeiro o IP/porta para testes com o Kafka, " +
                    "no segundo o Topic que receberá a mensagem, " +
                    "já no terceito em diante as mensagens a serem " +
                    "enviadas a um Topic no Kafka...");
                return;
            }

            string bootstrapServers = args[1];
            string nomeTopic = args[2];

            logger.Information($"BootstrapServers = {bootstrapServers}");
            logger.Information($"Topic = {nomeTopic}");

            try
            {
                var config = new ProducerConfig
                {
                    BootstrapServers = bootstrapServers
                };

                using (var producer = new ProducerBuilder<Null, string>(config).Build())
                {
                    for (int i = 3; i < args.Length; i++)
                    {
                        var result = await producer.ProduceAsync(
                            nomeTopic,
                            new Message<Null, string>
                            { Value = args[i] });

                        logger.Information(
                            $"Mensagem: {args[i]} | " +
                            $"Status: {result.Status.ToString()}");
                    }
                }

                logger.Information("Concluído o envio de mensagens");
            }
            catch (Exception ex)
            {
                logger.Error($"Exceção: {ex.GetType().FullName} | " +
                             $"Mensagem: {ex.Message}");
            }
        }

        static async Task Consumer(string[] args)
        {
            var logger = new LoggerConfiguration()
                 .WriteTo.Console()
                 .CreateLogger();
            logger.Information("Testando o consumo de mensagens com Kafka");

            if (args.Length < 2)
            {
                logger.Error(
                    "Informe 2 parâmetros: " +
                    "no primeiro o IP/porta para testes com o Kafka, " +
                    "no segundo o Topic a ser utilizado no consumo das mensagens...");
                return;
            }

            string bootstrapServers = args[1];
            string nomeTopic = args[2];

            logger.Information($"BootstrapServers = {bootstrapServers}");
            logger.Information($"Topic = {nomeTopic}");

            var config = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = $"{nomeTopic}-group-0",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            CancellationTokenSource cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            try
            {
                using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
                {
                    consumer.Subscribe(nomeTopic);

                    try
                    {
                        while (true)
                        {
                            var cr = consumer.Consume(cts.Token);
                            logger.Information(
                                $"Mensagem lida: {cr.Message.Value}");
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        consumer.Close();
                        logger.Warning("Cancelada a execução do Consumer...");
                    }
                }
            }
            catch (Exception ex)
            {
                logger.Error($"Exceção: {ex.GetType().FullName} | " +
                             $"Mensagem: {ex.Message}");
            }

        }
    }
}
