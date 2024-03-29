module AgentWebCrawler

open System
open System.Threading
open System.Collections.Concurrent
open System.Collections.Generic
open HtmlAgilityPack
open System.IO
open System.Net
open System.Text.RegularExpressions
open Common

module AgentCompositionIsDifficult =

    let cts = new CancellationTokenSource()
        
    let fetchContetAgent (limit : int option) (agent: Agent<Msg<string, string>>) =
        let token = cts.Token
        let agent = Agent<Msg<string, string>>.Start((fun inbox ->
            let rec loop (urls : Set<string>) = async {
                let! msg = inbox.Receive()
                match msg with
                | Item(url) -> 
                    if urls |> Set.contains url |> not then 
                        let! content = downloadContent url
                        content |> Option.iter(fun c -> 
                                agent.Post (Item(c)))
                        
                        let urls' = (urls |> Set.add url)
                        match limit with
                        | Some l when urls' |> Seq.length >= l -> cts.Cancel()
                        | _ -> return! loop urls'
            }
            loop Set.empty ), cancellationToken = token)
        token.Register(fun () -> (agent :> IDisposable).Dispose()) |> ignore
        agent
    
    let broadcastAgent (agents: Agent<Msg<string, string>> []) =
        let token = cts.Token
        let agent = Agent<Msg<string, string>>.Start((fun inbox -> 
            let rec loop () = async {
                let! msg = inbox.Receive()
                match msg with
                | Item(item) ->  
                    for agent in agents do
                        agent.Post(Item(item))
                    return! loop ()
                
            }
            loop ()), cancellationToken = token)
        token.Register(fun () -> (agent :> IDisposable).Dispose()) |> ignore
        agent
        
    let imageParserAgent (agent: Agent<Msg<string, string>>) =
        let token = cts.Token
        let agent = Agent<Msg<string, string>>.Start((fun inbox ->
            let rec loop () = async {
                let! msg = inbox.Receive()
                match msg with
                | Item(html) ->
                    let doc = new HtmlDocument()
                    doc.LoadHtml(html)
                            
                    let imageLinks =
                        doc.DocumentNode.Descendants("img")
                        |> Seq.choose(fun n ->
                            if n.Attributes.Contains("src") then
                                n.GetAttributeValue("src", "") |> Some
                            else None)
                        |> Seq.filter(fun url -> httpRgx.Value.IsMatch(url))
                                
                    for imgLink in imageLinks do
                       agent.Post (Item(imgLink))
                    
                    return! loop ()
            }
            loop ()), cancellationToken = token)
        token.Register(fun () -> (agent :> IDisposable).Dispose()) |> ignore
        agent
        
    let linksParserAgent (agent: Agent<Msg<string, string>>) =
        let token = cts.Token
        let agent = Agent<Msg<string, string>>.Start((fun inbox ->
            let rec loop () = async {            
                let! msg = inbox.Receive()
                match msg with
                | Item(html) ->
                    let doc = new HtmlDocument()
                    doc.LoadHtml(html)
                            
                    let links =
                        doc.DocumentNode.Descendants("a")
                        |> Seq.choose(fun n ->
                            if n.Attributes.Contains("href") then
                                n.GetAttributeValue("href", "") |> Some
                            else None)
                        |> Seq.filter(fun url -> httpRgx.Value.IsMatch(url))
                                
                    for link in links do
                        agent.Post (Item(link))
                    
                    return! loop ()
                
            }
            loop ()), cancellationToken = token)
        token.Register(fun () -> (agent :> IDisposable).Dispose()) |> ignore
        agent
        
    let comparison = StringComparison.InvariantCultureIgnoreCase                
    let linkFilter =
        fun (link : string) ->
            link.IndexOf(".aspx", comparison) <> -1 ||
            link.IndexOf(".php", comparison) <> -1 ||
            link.IndexOf(".htm", comparison) <> -1 ||
            link.IndexOf(".html", comparison) <> -1        
       
    let imageSideEffet (f: string -> byte[] -> Async<unit>) =
        let token = cts.Token
        let agent = Agent<Msg<string, _>>.Start((fun inbox ->
            let rec loop () = async {
                let! msg = inbox.Receive()
                match msg with
                | Item(url) ->
                    if linkFilter url then 
                        let client = new WebClient()
                        let! buffer = client.DownloadDataTaskAsync(url) |> Async.AwaitTask
                        do! f url buffer
                | _ -> failwith "no implemented"
                return! loop ()
            }
            loop ()), cancellationToken = token)
        token.Register(fun () -> (agent :> IDisposable).Dispose()) |> ignore
        agent
        
   
    type WebCrawler (?limit) as this =
        let fetchContetAgent = fetchContetAgent limit
        
        
        // How to compose agents, since are self rerencing each other
        // the order by which Agents are created manner
        
        
        member __.Dispose() = cts.Cancel()
      
        interface IDisposable with
            member x.Dispose() = this.Dispose()
    


module SyncWebCrawler =

    let cts = new CancellationTokenSource()
        
    let fetchContetAgent (limit : int option) =
        let token = cts.Token
        let agent = Agent<Msg<string, string>>.Start((fun inbox ->
            let rec loop (urls : Set<string>) (agents : Agent<_> list) = async {
                let! msg = inbox.Receive()
                
                match msg with
                | Item(url) -> 
                    if urls |> Set.contains url |> not then 
                        let! content = downloadContent url
                        content |> Option.iter(fun c -> 
                            for agent in agents do
                                agent.Post (Item(c)))
                        
                        let urls' = (urls |> Set.add url)
                        match limit with
                        | Some l when urls' |> Seq.length >= l -> cts.Cancel()
                        | _ -> return! loop urls' agents
                    else return! loop urls agents
                | Mailbox(agent) -> return! loop urls (agent::agents)
            }
            loop Set.empty []), cancellationToken = token)
        token.Register(fun () -> (agent :> IDisposable).Dispose()) |> ignore
        agent
    
    let broadcastAgent () =
        let token = cts.Token
        let agent = Agent<Msg<string, string>>.Start((fun inbox -> 
            let rec loop (agents : Agent<_> list) = async {
                let! msg = inbox.Receive()
                match msg with
                | Item(item) ->  
                    for agent in agents do
                        agent.Post(Item(item))
                    return! loop agents
                | Mailbox(agent) -> return! loop (agent::agents)
            }
            loop []), cancellationToken = token)
        token.Register(fun () -> (agent :> IDisposable).Dispose()) |> ignore
        agent
        
    let imageParserAgent () =
        let token = cts.Token
        let agent = Agent<Msg<string, string>>.Start((fun inbox ->
            let rec loop (agents : Agent<Msg<string, string>> list) = async {
                let! msg = inbox.Receive()
                match msg with
                | Item(html) ->
                    let doc = new HtmlDocument()
                    doc.LoadHtml(html)
                            
                    let imageLinks =
                        doc.DocumentNode.Descendants("img")
                        |> Seq.choose(fun n ->
                            if n.Attributes.Contains("src") then
                                n.GetAttributeValue("src", "") |> Some
                            else None)
                        |> Seq.filter(fun url -> httpRgx.Value.IsMatch(url))
                                
                    for imgLink in imageLinks do
                        agents |> Seq.iter(fun agent -> agent.Post (Item(imgLink)))
                    
                    return! loop agents
                | Mailbox(agent) -> return! loop (agent::agents)
            }
            loop []), cancellationToken = token)
        token.Register(fun () -> (agent :> IDisposable).Dispose()) |> ignore
        agent
        
    let linksParserAgent () =
        let token = cts.Token
        let agent = Agent<Msg<string, string>>.Start((fun inbox ->
            let rec loop (agents : Agent<_> list) = async {            
                let! msg = inbox.Receive()
                match msg with
                | Item(html) ->
                    let doc = new HtmlDocument()
                    doc.LoadHtml(html)
                            
                    let links =
                        doc.DocumentNode.Descendants("a")
                        |> Seq.choose(fun n ->
                            if n.Attributes.Contains("href") then
                                n.GetAttributeValue("href", "") |> Some
                            else None)
                        |> Seq.filter(fun url -> httpRgx.Value.IsMatch(url))
                                
                    for link in links do
                        agents |> Seq.iter(fun agent -> agent.Post (Item(link)))
                    
                    return! loop agents
                | Mailbox(agent) -> return! loop (agent::agents)
            }
            loop []), cancellationToken = token)
        token.Register(fun () -> (agent :> IDisposable).Dispose()) |> ignore
        agent
        
    let comparison = StringComparison.InvariantCultureIgnoreCase                
    let linkFilter =
        fun (link : string) ->
            link.IndexOf(".aspx", comparison) <> -1 ||
            link.IndexOf(".php", comparison) <> -1 ||
            link.IndexOf(".htm", comparison) <> -1 ||
            link.IndexOf(".html", comparison) <> -1        
       
    let imageSideEffet (f: string -> byte[] -> Async<unit>) =
        let token = cts.Token
        let agent = Agent<Msg<string, _>>.Start((fun inbox ->
            let rec loop () = async {
                let! msg = inbox.Receive()
                match msg with
                | Item(url) ->
                    if linkFilter url then 
                        let client = new WebClient()
                        let! buffer = client.DownloadDataTaskAsync(url) |> Async.AwaitTask
                        do! f url buffer
                | _ -> failwith "no implemented"
                return! loop ()
            }
            loop ()), cancellationToken = token)
        token.Register(fun () -> (agent :> IDisposable).Dispose()) |> ignore
        agent
        
    let saveImageAgent =
        imageSideEffet (fun url buffer -> async {
                let fileName = Path.GetFileName(url)
                let name = @"Images\" + fileName
                printfn "Name : %s" name
                //use stream = File.OpenWrite(name)
                //do! stream.AsyncWrite(buffer)
            })            

    type WebCrawler (?limit) as this =
        let fetchContetAgent = fetchContetAgent limit
        let contentBroadcaster = broadcastAgent ()
        let linkBroadcaster = broadcastAgent ()
        let imageParserAgent = imageParserAgent ()
        let linksParserAgent = linksParserAgent ()
        
        do  
            fetchContetAgent.Post (Mailbox(contentBroadcaster))    
            contentBroadcaster.Post (Mailbox(imageParserAgent))
            contentBroadcaster.Post (Mailbox(linksParserAgent))
            contentBroadcaster.Post (Mailbox(printerAgent cts.Token))
            linkBroadcaster.Post (Mailbox(printerAgent cts.Token))
            imageParserAgent.Post (Mailbox(saveImageAgent))        
            linksParserAgent.Post (Mailbox(linkBroadcaster))
            linkBroadcaster.Post (Mailbox(saveImageAgent))
            linkBroadcaster.Post (Mailbox(fetchContetAgent))
        
        member __.Submit(url : string) = fetchContetAgent.Post(Item(url))
        
        member __.Dispose() = cts.Cancel()
      
        interface IDisposable with
            member x.Dispose() = this.Dispose()
    

