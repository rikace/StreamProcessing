module Reactive.StreamTwo

open System
open Akka
open Akka.Streams
open Akka.Streams.Dsl
open Tweetinvi.Models
open Shared.Reactive
open System.Linq
open System.Xml.Linq
open System.Threading.Tasks
open StreamingCombinators

// Function overloading using infix operator
// Function overloading is  a syntactic construct where we define two or more different functions with the same name
type SinkHelper = SinkHelper with
    static member (==>) (_:SinkHelper, a:GraphDsl.ForwardOps<_, NotUsed>) = fun(b:Inlet<string>) -> a.To(b)
    static member (==>) (_:SinkHelper, a:Sink<_,Task>) = fun(b:Source<string, _>) -> b.To(a)
    static member (==>) (_:SinkHelper, a:Source<_,_>) = fun(b:Sink<_,Task>) ->a.To(b)
    static member (==>) (_:SinkHelper, b:Inlet<string>) = fun(a:GraphDsl.ForwardOps<_, NotUsed>) -> a.To(b)
let inline sink x = SinkHelper ==> x


type MatHelpler = MatHelpler with
    static member (==>) (_:MatHelpler, a:IGraph<FlowShape<_, _>, NotUsed>) = fun (b:Source<ITweet, _>) -> b.Via(a)
let inline mat x = MatHelpler ==> x

type ViaHelper = ViaHelper with
    static member (==>) (_:ViaHelper, a:IGraph<FlowShape<_, _>, NotUsed>) = fun (b:GraphDsl.ForwardOps<_, NotUsed>) -> b.Via(a)
    static member (==>) (b:ViaHelper, a:GraphDsl.ForwardOps<_, NotUsed>) = fun (x:IGraph<FlowShape<_, _>, NotUsed>) -> a.Via(x)
    static member (==>) (b:ViaHelper, a:Source<ITweet, _>) = fun(x:IGraph<FlowShape<_, _>,_>) -> a.Via(x)
let inline via x = ViaHelper ==> x


module TweetsWithThrottle =

    let create<'a>(tweetSource:Source<ITweet, 'a>) effect =
        let formatUser =
            Flow.Create<IUser>()
            |> FlowEx.select (Utils.FormatUser)

        let formatCoordinates =
            Flow.Create<ICoordinates>()
            |> FlowEx.select (Utils.FormatCoordinates)

        let flowCreateBy =
            Flow.Create<ITweet>()
            |> FlowEx.select (fun tweet -> tweet.CreatedBy)

        let flowCoordinates =
            Flow.Create<ITweet>()
            |> FlowEx.select (fun tweet -> tweet.Coordinates)

        let writeSink = Sink.ForEach<string>(fun text -> effect text)

        let graph = GraphDsl.Create(fun buildBlock ->
            let broadcast = buildBlock.Add(Broadcast<ITweet>(2))
            let merge = buildBlock.Add(Merge<string>(2))

            buildBlock.From(broadcast.Out(0))
            |> via (flowCreateBy
            |> FlowEx.throttle 10 (TimeSpan.FromSeconds(1.)) 1 ThrottleMode.Shaping)
            |> via formatUser
            |> sink (merge.In(0))
            |> ignore

            buildBlock.From(broadcast.Out(1))
            |> via (flowCoordinates
                        |> FlowEx.throttle 10 (TimeSpan.FromSeconds(1.)) 10 ThrottleMode.Shaping)
            |> via formatCoordinates
            |> sink (merge.In(1))
            |> ignore

            FlowShape<ITweet, string>(broadcast.In, merge.Out))

        tweetSource
        |> FlowEx.where (Predicate(fun (tweet:ITweet) -> not(isNull tweet.Coordinates)))
        |> mat graph
        |> sink writeSink
        

module TweetsWeatherWithThrottle =
    open System.Threading.Tasks

    let create<'a>(tweetSource:Source<ITweet, 'a>) effect : IRunnableGraph<'a> =
        
        let formatUser =
            Flow.Create<IUser>()
            |> FlowEx.select (Utils.FormatUser)

        let formatTemperature =
            Flow.Create<decimal>()
            |> FlowEx.select (Utils.FormatTemperature)

        let xn s = XName.Get s
        let getWeatherAsync(coordinates:ICoordinates) =
            async {
                use httpClient = new System.Net.WebClient()
                let requestUrl = sprintf "http://api.met.no/weatherapi/locationforecast/1.9/?lat=%f;lon=%f" coordinates.Latitude coordinates.Latitude
                printfn "%s" requestUrl

                let! result = httpClient.DownloadStringTaskAsync (Uri requestUrl) |> Async.AwaitTask
                let doc = XDocument.Parse(result)
                let temp = doc.Root.Descendants(xn "temperature").First().Attribute(xn "value").Value
                return Decimal.Parse(temp)
            } |> Async.StartAsTask

        let writeSink =
            Sink.ForEach<string>(fun msg -> effect msg)

        let selectAsync (parallelism:int) (asyncMapper:_ -> Task<_>) (flow:Flow<_, _,NotUsed>) =
            flow.SelectAsync(parallelism, Func<_, Task<_>>(asyncMapper))

        let graph = GraphDsl.Create(fun buildBlock ->
            let broadcast = buildBlock.Add(Broadcast<ITweet>(2))
            let merge = buildBlock.Add(Merge<string>(2))

            buildBlock.From(broadcast.Out(0))
            |> via (Flow.Create<ITweet>()
                    |> FlowEx.select (fun tweet -> tweet.CreatedBy)
                    |> FlowEx.throttle 10 (TimeSpan.FromSeconds(1.)) 1 ThrottleMode.Shaping)
            |> via (formatUser)
            |> sink (merge.In(0))
            |> ignore

            buildBlock.From(broadcast.Out(1))
            |> via (Flow.Create<ITweet>()
                    |> FlowEx.select (fun tweet -> tweet.Coordinates)
                    
                // 1- Throttle (Throttle(10) >> same rate
                // 2- Throttle (Throttle(1))
                //         2 channels with a difference throttle values
                //         1 request with 10 msg per second and 1 request with 1 msg per second
                //         because there is only 1 stream source, it cannot send messages to a
                //         different rate, thus, it satisfies the lowest requirement.
                    
                    |> FlowEx.throttle 1 (TimeSpan.FromSeconds(1.)) 1 ThrottleMode.Shaping)
                    |> via (Flow.Create<ICoordinates>()
                             // selectAsync - Runs in parallel up to 4
                             |> selectAsync 4 (fun c -> Utils.GetWeatherAsync(c)))
            |> via (formatTemperature)
            |> sink (merge.In(1))
            |> ignore

            FlowShape<ITweet, string>(broadcast.In, merge.Out))

        tweetSource
        |> FlowEx.where (Predicate(fun (tweet:ITweet) -> not(isNull tweet.Coordinates)))
        |> mat graph
        |> sink writeSink

module TweetsWithWeather =
    
    let xn s = XName.Get s
    let getWeatherAsync(coordinates:ICoordinates) =
        async {
            use httpClient = new System.Net.WebClient()
            let requestUrl = sprintf "http://api.met.no/weatherapi/locationforecast/1.9/?lat=%f;lon=%f" coordinates.Latitude coordinates.Latitude
            printfn "%s" requestUrl

            let! result = httpClient.DownloadStringTaskAsync (Uri requestUrl) |> Async.AwaitTask
            let doc = XDocument.Parse(result)
            let temp = doc.Root.Descendants(xn "temperature").First().Attribute(xn "value").Value
            return Decimal.Parse(temp)
        } |> Async.StartAsTask


    let create<'a>(tweetSource:Source<ITweet, 'a>) effect =

        let formatUser =
            Flow.Create<IUser>()
            |> FlowEx.select (Utils.FormatUser)

        let formatCoordinates =
            Flow.Create<ICoordinates>()
            |> FlowEx.select (Utils.FormatCoordinates)

        let formatTemperature =
            Flow.Create<decimal>()
            |> FlowEx.select (Utils.FormatTemperature)

        let createBy = Flow.Create<ITweet>().Select(fun tweet -> tweet.CreatedBy)

        let writeSink =
            Sink.ForEach<string>(fun msg -> effect msg)

        let selectAsync (parallelism:int) (asyncMapper:_ -> Task<_>) (flow:Flow<_, _,NotUsed>) =
            flow.SelectAsync(parallelism, Func<_, Task<_>>(asyncMapper))

        let graph = GraphDsl.Create(fun buildBlock ->
            let broadcast = buildBlock.Add(Broadcast<ITweet>(2))
            let merge = buildBlock.Add(Merge<string>(2))

            buildBlock.From(broadcast.Out(0))
            |> via (Flow.Create<ITweet>()
                    |> FlowEx.select (fun tweet -> tweet.CreatedBy)
                    |> FlowEx.throttle 10 (TimeSpan.FromSeconds(1.)) 1 ThrottleMode.Shaping)
            |> via (formatUser)
            |> sink (merge.In(0))
            |> ignore

            buildBlock.From(broadcast.Out(1))
            |> via (Flow.Create<ITweet>()
                    |> FlowEx.select (fun tweet -> tweet.Coordinates)
                    |> FlowEx.buffer 10 OverflowStrategy.DropNew
                    |> FlowEx.throttle 1 (TimeSpan.FromSeconds(1.)) 1 ThrottleMode.Shaping)
                    |> via (Flow.Create<ICoordinates>()
                             |> selectAsync 4
                                (fun c -> Utils.GetWeatherAsync(c)))
            |> via (formatTemperature)
            |> sink (merge.In(1))
            |> ignore

            FlowShape<ITweet, string>(broadcast.In, merge.Out))

        tweetSource
        |> FlowEx.where (Predicate(fun (tweet:ITweet) -> not(isNull tweet.Coordinates)))
        |> mat graph
        |> sink writeSink

