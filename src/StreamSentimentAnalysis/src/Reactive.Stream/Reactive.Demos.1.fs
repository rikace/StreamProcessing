module Reactive.StreamOne

open System
open Akka.Streams
open Akka.Streams.Dsl
open Tweetinvi.Models
open Shared.Reactive
open StreamingCombinators
                                                                                  
module TweetsToConsole =
    open FlowEx
    // Simple implementation that read ~70Mb tweets from memory (load the tweets from local file-system) to generate high throughput
    
    let inline create<'a>(tweetSource :Source<ITweet, 'a>) : IRunnableGraph<'a> =

        let formatFlow =
            Flow.Create<ITweet>()
            |> select (Utils.FormatTweet)

        let writeSink = Sink.ForEach<string>(fun text -> Console.WriteLine(text))

        tweetSource.Via(formatFlow).To(writeSink)

module TweetsWithBroadcast =
   open FlowEx
    
   let inline create(tweetSource:Source<ITweet, 'a>) =
        let formatUser =
            Flow.Create<IUser>()
            |> select (Utils.FormatUser)

        let formatCoordinates =
            Flow.Create<ICoordinates>()
            |> select (Utils.FormatCoordinates)

        let flowCreateBy =
            Flow.Create<ITweet>()
            |> select (fun tweet -> tweet.CreatedBy)

        let flowCoordinates =
            Flow.Create<ITweet>()
            |> select (fun tweet -> sprintf "Lat %f - Lng %f" tweet.Coordinates.Latitude tweet.Coordinates.Longitude)

        let writeSink = Sink.ForEach<string>(fun text -> Console.WriteLine(text))

        let graph = GraphDsl.Create(fun buildBlock ->
            let broadcast = buildBlock.Add(Broadcast<ITweet>(2))
            let merge = buildBlock.Add(Merge<string>(2))

            buildBlock.From(broadcast.Out(0)).Via(flowCreateBy).Via(formatUser).To(merge.In(0)) |> ignore
            buildBlock.From(broadcast.Out(1)).Via(flowCoordinates).To(merge.In(1)) |> ignore
            FlowShape<ITweet, string>(broadcast.In, merge.Out))
        
         
        tweetSource
        |> where (Predicate(fun (tweet:ITweet) -> not(isNull tweet.Coordinates)))
        |> viaGraph graph
        |> sinkGraph writeSink
        