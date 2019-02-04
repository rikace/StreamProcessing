﻿

open System
open System.Collections.Generic
open System.Configuration
open Akka
open Akka.Actor
open Akka.Streams
open Tweetinvi
open Tweetinvi.Models
open Akka.Streams.Dsl
open Shared.Reactive
open Shared.Reactive.Tweets
open Reactive.StreamTwo
open System.Threading.Tasks
open Reactive.StreamOne
open Reactive.StreamTwo

type RunnableGraphType =
    | TweetsToConsole
    | TweetsWithBroadcast
    | TweetsWithThrottle
    | TweetsWithWeather
    | TweetsWithWeatherThrottle
    | TweetsToEmotion
    
module Graph =
    let inline graph<'a>(tweetSource:Source<ITweet, 'a>) grapType =
        match grapType with
        // Simple implementation that read ~60Mb tweets from memory (load the tweets from local file-system) to generate high throughput (thus, it becomes easier to generate back-pressure)

        | RunnableGraphType.TweetsToConsole -> TweetsToConsole.create(tweetSource)
        // The Graph reads the source of events and generates 2 channels
        // 1 channel formats the output to render only the User of the tweet
        // 2 channel formats the output to render only the coordinates
        // Ultimately the channels are merged together and rendered at the same rate
        | RunnableGraphType.TweetsWithBroadcast -> Reactive.StreamOne.TweetsWithBroadcast.create(tweetSource)
        // Similar to TweetsWithBroadcast but with Throttling
        | RunnableGraphType.TweetsWithThrottle -> TweetsWithThrottle.create(tweetSource)
        | RunnableGraphType.TweetsWithWeather -> TweetsWithWeather.create(tweetSource)
        | RunnableGraphType.TweetsWithWeatherThrottle -> TweetsWeatherWithThrottle.create(tweetSource)
        // Tweet Emotion in comparison to StockTicker example
       

let runTweetStreaming useCachedTweets (graphType : RunnableGraphType) =
    use system = ActorSystem.Create("Reactive-System")
    let consumerKey = ConfigurationManager.AppSettings.["ConsumerKey"]
    let consumerSecret = ConfigurationManager.AppSettings.["ConsumerSecret"]
    let accessToken = ConfigurationManager.AppSettings.["AccessToken"]
    let accessTokenSecret = ConfigurationManager.AppSettings.["AccessTokenSecret"]

    Console.OutputEncoding <- System.Text.Encoding.UTF8
    Console.ForegroundColor <- ConsoleColor.Cyan

    Console.WriteLine("<< Press Enter to Start >>")
    Console.ReadLine() |> ignore
 
    use materialize = system.Materializer()

    if useCachedTweets then

        let tweetSource = Source.FromEnumerator(fun () -> (new TweetEnumerator(true)) :> IEnumerator<ITweet>)
        let graph = Graph.graph<NotUsed>(tweetSource) graphType
        graph.Run(materialize) |> ignore

    else
        Auth.SetCredentials(new TwitterCredentials(consumerKey, consumerSecret, accessToken, accessTokenSecret))

            // TODO OverflowStrategy.DropHead 
        let tweetSource = Source.ActorRef<ITweet>(100, OverflowStrategy.DropBuffer)
        let graph = Graph.graph<IActorRef>(tweetSource) graphType
        let actor = graph.Run(materialize)

        Utils.StartSampleTweetStream(actor)
    
    
[<EntryPoint>]
let main argv =
    
    let runWebCrawler () =
        Task.Run(fun () ->
            WebCrawler.run [ "http://cnn.com/"; "http://bbc.com/"; "http://google.com/" ])
        |> ignore
        
    let runTweetStreamimg () =
        runTweetStreaming true RunnableGraphType.TweetsToConsole 
        

    runWebCrawler()
    runTweetStreamimg()
    
    Console.WriteLine("<< Press Enter to Exit >>")
    Console.ReadLine() |> ignore
    
    0