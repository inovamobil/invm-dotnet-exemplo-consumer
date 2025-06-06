using RabbitMQ.Client;
using RabbitMQ.Client.Events;

Console.WriteLine("Iniciando escuta e tratamento de mensagens... \n \n \n");

//// Exemplo de consumidor RabbitMQ usando RabbitMQ.Client
var factory = new ConnectionFactory
{
    HostName = "localhost", //Fornecido pela Inovamobil
    UserName = "0001", //Fornecido pela Inovamobil - Será um por cliente e o mesmo nome do virtualhost
    Password = "",//Fornecido pela Inovamobil - Será um por cliente
    VirtualHost = "0001" // Código do cliente fornecido pela Inovamobil
};


//// Criar uma conexão e um canal RabbitMQ
using var connection = await factory.CreateConnectionAsync();
using var channel = await connection.CreateChannelAsync();

// Declarar (Criar) a fila QuitacaoInovapay se não existir
await channel.QueueDeclareAsync(queue: "QuitacaoInovapay", //Esse sempre será o nome da fila
                                durable: true,
                                exclusive: false,
                                autoDelete: false,
                                arguments: null);

var consumer = new AsyncEventingBasicConsumer(channel);

// Assinar o evento ReceivedAsync para processar mensagens recebidas
consumer.ReceivedAsync += async (model, ea) =>
{
    var body = ea.Body.ToArray();
    var message = System.Text.Encoding.UTF8.GetString(body);

    // Exibir a mensagem
    Console.WriteLine($"Mensagem recebida: {message}");

    try
    {
        // Simular Processamento da mensagem recebida
        await Task.Delay(1000);

        // Acknowledge a mensagem
        await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
    }
    catch
    {
        // Em caso de erro, rejeitar a mensagem sem requeue
        await channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: true);
        Console.WriteLine("Erro ao processar a mensagem. Mensagem rejeitada.");
    }
};

// Definir o consumidor para receber mensagens da fila
await channel.BasicConsumeAsync(queue: "QuitacaoInovapay",
                                autoAck: false,
                                consumer: consumer);

Console.WriteLine("Pressione qualquer tecla para sair.");
Console.ReadLine();
