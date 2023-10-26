﻿using System.Collections.Specialized;
using System.Configuration;
using MassTransit;
using MassTransit.Util;
using Unity;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Unity.Microsoft.DependencyInjection;

namespace MassTransitTester
{
    internal class Program
    {
        public static IUnityContainer Container = new UnityContainer();
        private static ILoggerFactory _loggerFactory;

        private static void Main(string[] args)
        {
            IBusControl busControl = null;

            _loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.AddConsole();
            });

            OutputAppSettings();

            NameValueCollection appSettings = ConfigurationManager.AppSettings;

            var uri = new Uri(appSettings["Uri"] + appSettings["Host"] + "/" + appSettings["VirtualHost"] + "/");

            try
            {
                busControl = Create(busConfig =>
                {
                    busConfig.Host(uri,
                        configurator =>
                        {
                            configurator.Username(appSettings["Username"]);
                            configurator.Password(appSettings["Password"]);

                            string[] clusterMembers = { };

                            clusterMembers = appSettings["ClusterNames"]
                                .Split(',');

                            if (clusterMembers.Length > 0)
                            {
                                configurator.UseCluster(c =>
                                {
                                    foreach (string clusterMember in clusterMembers)
                                    {
                                        c.Node(clusterMember);
                                    }
                                });
                            }
                        });

                    LogContext.ConfigureCurrentLogContext(_loggerFactory);

                    busConfig.SetQuorumQueue();
                    var serviceCollection = new ServiceCollection();

                    busConfig.ReceiveEndpoint(appSettings["QueueName"],
                        ec =>
                        {
                            ec.SetQuorumQueue();
                            ec.Consumer<TestMessageConsumer>(
                                serviceCollection.BuildServiceProvider(Container));
                        });
                });

                Container.RegisterInstance(busControl);
                Container.RegisterInstance<IBus>(busControl);
                Container.RegisterInstance<ISendEndpointProvider>(busControl);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }

            busControl.StartAsync().GetAwaiter();

            ISendEndpoint? sendEndpoint = busControl.GetSendEndpoint(new Uri(uri + appSettings["QueueName"]))
                                                    .GetAwaiter()
                                                    .GetResult();

            var quit = false;

            while (quit == false)
            {
                Console.WriteLine("Please enter a message or \"q\" to exit");
                try
                {
                    var input = Console.ReadLine();

                    if (input != null)
                    {
                        if (input == "q")
                            quit = true;
                        else
                            sendEndpoint.Send(new TestMessage {
                                Message = input
                            });

                        TaskUtil.Await(() => Task.Delay(500));
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                }
            }

            Console.WriteLine("Press any key to exit.");
            Console.ReadKey();
        }

        public static IBusControl Create(Action<IRabbitMqBusFactoryConfigurator> configure)
        {
            return Bus.Factory.CreateUsingRabbitMq(configure);
        }

        private static void OutputAppSettings()
        {
            try
            {
                var appSettings = ConfigurationManager.AppSettings;

                if (appSettings.Count == 0)
                {
                    Console.WriteLine("AppSettings is empty.");
                }
                else
                {
                    foreach (var key in appSettings.AllKeys)
                    {
                        Console.WriteLine("Key: {0} Value: {1}", key, appSettings[key]);
                    }
                }
            }
            catch (ConfigurationErrorsException)
            {
                Console.WriteLine("Error reading app settings");
            }
        }
    }

    internal class TestMessage
    {
        public string Message { get; set; }
    }

    internal class TestMessageConsumer : IConsumer<TestMessage>
    {
        public Task Consume(ConsumeContext<TestMessage> context)
        {
            Console.WriteLine($"Received: {context.Message.Message}");
            return Task.CompletedTask;
        }
    }
}