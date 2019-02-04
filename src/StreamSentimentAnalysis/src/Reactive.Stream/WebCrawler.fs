module WebCrawler

open System
open System.Linq
open System.Collections.Immutable
open System.Text.RegularExpressions
open System.IO
open FSharp.Core
open System.Diagnostics
open System.Net
open System.Net.Http
open System.Threading
open System.Threading.Tasks
open Akka
open Akka.Actor
open Akka.FSharp
open Akka.Streams
open Akka.Streams.Dsl
open Akka.Streams.Implementation.Fusing
open Akka.Util
open Microsoft.FSharp.Core.Printf
open System.Threading.Tasks
open Microsoft.FSharp.Control
open Akkling
open Akkling.Streams
open CsQuery

let resolveLinks (uri : Uri, cq : CQ) = seq {
    for link in cq.["a[href]"] do
       // printfn "link - %O" link
        let href = link.GetAttribute("href")
      //  printfn "href - %O" href
        let mutable uriResult = Unchecked.defaultof<Uri>
        if Uri.TryCreate(href, UriKind.Absolute, &uriResult) then
        //    printfn "resolveLinks -1- %O..." uriResult
            yield uriResult
        elif Uri.TryCreate(uri, href, &uriResult) then
        //    printfn "resolveLinks -2- %O..." uriResult
            yield uriResult
    }

let downloadPage (uri : Uri) = 
    printfn "downloading %O..." uri 
    let req = WebRequest.CreateHttp(uri)
    let resp = req.GetResponse()
    use stream = resp.GetResponseStream()
    if isNull stream then uri, CQ()
    else
        use reader = new StreamReader(stream)
        let html = reader.ReadToEnd()
        uri, CQ.CreateDocument(html)
       

let downloadPageAsync (uri : Uri) = async {
    printfn "downloading %O..." uri 
    let req = WebRequest.CreateHttp(uri)
    let! resp = req.GetResponseAsync() |> Async.AwaitTask
    use stream = resp.GetResponseStream()
    if isNull stream then return uri, CQ()
    else
        use reader = new StreamReader(stream)
        let! html = reader.ReadToEndAsync() |> Async.AwaitTask
        return uri, CQ.CreateDocument(html)
        }

    

let webCrawler () : IGraph<FlowShape<Uri, Uri>, NotUsed> =
    let index = ConcurrentSet<Uri>()
    let graph = Graph.create (fun b ->
    
        let merge = b.Add(new MergePreferred<Uri>(1))
        let bcast = b.Add(new Broadcast<Uri>(2))
        
        // async downlad page from provided uri
        // resolve links from it        
        let flow =
            Flow.empty<Uri, _>
            |> Flow.filter(fun uri -> index.TryAdd(uri))
            |> Flow.asyncMapUnordered 4 downloadPageAsync
            |> Flow.collect resolveLinks

        // feedback loop - take only those elements,
        // which were successfully added to index (unique)
        let flowBack =
            Flow.empty<Uri, _>
            |> Flow.choose(fun uri -> if index.Contains uri then None else Some uri)
            |> Flow.conflateSeeded (fun (uris : ImmutableList<Uri>) uri -> uris.Add uri) (fun uri -> ImmutableList.Create(uri))
            |> Flow.collect id
        
        let pipe = b.From(merge).Via(flow).To(bcast)
        b.From(bcast).Via(flowBack).To(merge.Preferred) |> ignore
        
        new FlowShape<Uri, Uri>(merge.In(0), bcast.Out(1)))
    
    graph        
        
            
let run urls =
    let sys = System.create("Reactive-WebCrawler") (Configuration.defaultConfig())
    let mat = sys.Materializer()
    
    let graph : RunnableGraph<NotUsed> =
        printfn "Starting graph..."
        Graph.create (fun b ->
            let source = b.Add(Source.From( urls |> Seq.map Uri).Async())
            let sink = b.Add(Sink.forEach (fun s -> printfn "%O" s))
            let crawlerFlow = b.Add(webCrawler().Async())
            
            b.From(source).Via(crawlerFlow).To(sink) |> ignore
            
            ClosedShape.Instance)
        |> RunnableGraph.FromGraph
        
    graph |> Graph.run mat
    Console.ReadLine() |> ignore
    
                    