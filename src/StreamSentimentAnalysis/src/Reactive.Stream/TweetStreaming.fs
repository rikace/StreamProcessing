module TweeterStreaming

open System
open System.Collections.Generic
open System.Text.RegularExpressions
open System.IO
open FSharp.Core
open System.Diagnostics
open System.Net
open System.Threading
open System.Threading.Tasks
open Akka
open Akka.Actor
open Akka.FSharp
open Akka.Streams
open Akka.Streams.Dsl
open Akka.Streams.Implementation.Fusing
open Microsoft.FSharp.Core.Printf
open System.Threading.Tasks
open Microsoft.FSharp.Control
open Akkling
open Akkling.Streams
open Akkling.Streams.Operators
open System.Configuration
open System.Text
open Tweetinvi.Models
open Tweetinvi
open Tweetinvi.Core.Interfaces
open Tweetinvi.Core.Parameters

module Credentials =
    let consumerKey = ConfigurationManager.AppSettings.["ConsumerKey"]
    let consumerSecret = ConfigurationManager.AppSettings.["ConsumerSecret"]
    let accessToken = ConfigurationManager.AppSettings.["AccessToken"]
    let accessTokenSecret = ConfigurationManager.AppSettings.["AccessTokenSecret"]
    
    
let formatTweet(tweet : ITweet) =
    let builder = new StringBuilder()
    builder.AppendLine("---------------------------------------------------------")
        .AppendLine(sprintf "Tweet from NewYork from: %s :" tweet.CreatedBy.Name)
        .AppendLine()
        .AppendLine(tweet.Text)
        .AppendLine("---------------------------------------------------------")
        .AppendLine() |> ignore
    builder.ToString()
        
let run2 () =
    let tweetSource = Source.ActorRef<ITweet>(100, OverflowStrategy.DropHead)
    let formatFlow = Flow.Create<ITweet>().Select(new Func<ITweet, string>(formatTweet))
    let writeSink = Sink.ForEach<string>(new Action<string>(Console.WriteLine))
    
    let countauthors = Flow.Create<ITweet>().StatefulSelectMany(new Func<_>(fun () ->
                        let dict = new Dictionary<string, int>()
    
                        let res =
                            fun (tweet : ITweet) ->
                                let user = tweet.CreatedBy.Name
                                if dict.ContainsKey user |> not then
                                    dict.Add(user, 0)
                                 
                                dict.[user] <- dict.[user] + 1
                                
                                (sprintf "%d tweets from user %s" dict.[user] user) |> Seq.singleton
    
                        new Func<ITweet,_>(res)))
    
    let notUsed = new Func<NotUsed,_, _>(fun notUsed _ -> notUsed)
    
    let builder =
        new Func<GraphDsl.Builder<NotUsed>, FlowShape<ITweet, string>, SinkShape<string>, _>(fun b count write ->
            let broadcast = b.Add(new Broadcast<ITweet>(2))
            let output = b.From(broadcast.Out(0)).Via(formatFlow)
            b.From(broadcast.Out(1)).Via(count).To(write) |> ignore
            new FlowShape<ITweet, string>(broadcast.In, output.Out))
        
    
    let graph = GraphDsl.Create(countauthors, writeSink, notUsed, builder)
    
    let sys = System.create("Reactive-Tweets") (Configuration.defaultConfig())    
    let mat = sys.Materializer()
    
    // Start Akka.Net stream
    let actor = tweetSource.Via(graph).To(writeSink).Run(mat)
    
    // Start Twitter stream
    Auth.SetCredentials(new TwitterCredentials(Credentials.consumerKey, Credentials.consumerSecret, Credentials.accessToken, Credentials.accessTokenSecret))
    let stream = Stream.CreateSampleStream()
    
    //stream.AddLocation(CenterOfNewYork)
    stream.TweetReceived |> Observable.add(fun arg -> actor.Tell(arg.Tweet)) // push the tweets into the stream
    stream.StartStream()

      
let runFun2 () =
    let tweetSource = Source.actorRef OverflowStrategy.DropHead 100
    let formatFlow = Flow.empty<ITweet, _> |> Flow.map formatTweet 
    let writeSink = Sink.forEach (fun tweet -> printfn "%s" tweet)
    
    let countauthors =
        Flow.empty<ITweet,NotUsed>
        |> Flow.statefulCollect (fun (state : Dictionary<string, int>) tweet ->           
                let user = tweet.CreatedBy.Name
                if state.ContainsKey user |> not then
                    state.Add(user, 0)
                state.[user] <- state.[user] + 1
                state, (sprintf "%d tweets from user %s" state.[user] user) |> Seq.singleton) (new Dictionary<string, int>())
  
    let graph = Graph.create2 (fun notUsed _ -> notUsed) (fun b (count : FlowShape<ITweet, string>) (writer : SinkShape<String>) ->
            let broadcast = b.Add(new Broadcast<ITweet>(2))
            let output = b.From(broadcast.Out(0)).Via(formatFlow)            
            b.From(broadcast.Out(1)).Via(count).To(writer) |> ignore
            new FlowShape<ITweet, string>(broadcast.In, output.Out))
                 countauthors writeSink
                 |> Flow.FromGraph
    
        
    let sys = System.create("Reactive-Tweets") (Configuration.defaultConfig())
    let mat = sys.Materializer()
    
    // Start Akka.Net stream
    let actor = tweetSource |> Source.via graph |> Source.toSink writeSink |> Graph.run mat
        
    // Start Twitter stream
    Auth.SetCredentials(new TwitterCredentials(Credentials.consumerKey, Credentials.consumerSecret, Credentials.accessToken, Credentials.accessTokenSecret))
    let stream = Stream.CreateSampleStream()
    
    stream.TweetReceived |> Observable.add(fun arg -> actor <! arg.Tweet)
    stream.StartStream()


