#r "nuget: Akka.FSharp"

open Akka
open System
open Akka.Actor
open Akka.FSharp
open System.Collections.Generic
open System.Diagnostics
open Akka.Actor
open Akka.Configuration
open Akka.FSharp
open System.Text
open System.Numerics
open System.Collections.Generic
open System.Security.Cryptography


let mutable endFlag = false
let inputNodes = int fsi.CommandLineArgs.[1]
let inputRequest = int fsi.CommandLineArgs.[2]
let mutable i:int = 0
let rand=System.Random()
let calculateM inputNodes: int= // calculate m value
    let mutable cal =1
    let mutable i=0 
    while inputNodes>cal do 
        cal<- 2*cal
        i<-i+1
    i

let generatehash (s1:string): string=  //generate hash 
  
    let hash1 = 
       s1
       |> Encoding.ASCII.GetBytes
       |> (new SHA1Managed()).ComputeHash
       |> System.BitConverter.ToString

    let finhash = hash1.Replace("-","")
    printfn"Hash: %s" finhash
    finhash   // return hash


let mutable fingerList = new Dictionary<int, List<int>>()
let mutable successorList = new Dictionary<int, int>()
let mutable predecessorList = new Dictionary<int, int>()
let mutable keyList = new Dictionary<int, List<int>>()
let activeList = new List<int>()
let tempList = new List<int>()
let system = ActorSystem.Create("FSharp")

type ActorMsg = 
           
            
            | RouteFinish of a: int
            | StartJoining
            | JoinNode of int
            | Acknowledgement
            | StartJoin of int * int 
            | Stabilize of int
            | Notify of int
            | FixFingers of int
            | CheckPredecessor of int
            | UpdateSuccessor 



let swap (a: _[]) x y =
    let tmp = a.[x]
    a.[x] <- a.[y]
    a.[y] <- tmp

let shuffle a =
    Array.iteri (fun i _ -> swap a i (rand.Next(i, Array.length a))) a
    a



let mutable temp:int = 0
let m: int = calculateM(inputNodes)
let mNode :int =int (pown 2 m)
let randomMapping = shuffle [|0 .. mNode-1|]
let ihops:float = float(inputNodes/10)* float(rand.NextDouble()+1.0)
let time = Math.Log(float(inputNodes)) / Math.Log(2.0) * 1.3 + rand.NextDouble()

let selectActor(id: int) = 
    let actorPath = "akka://FSharp/user/master/" + string id
    select actorPath system

let selectAllActors() = 
    system.ActorSelection("akka://FSharp/user/master/*")

let mutable refNode:int = -1
let mutable nNot:int =0


let childNode nodeID (mailbox: Actor<_>) =
    
    let rec loop() = actor {
        let! message = mailbox.Receive()
        let sender = mailbox.Sender()
        match message with
           
            
           | UpdateSuccessor ->
                
                activeList.Sort()
                for i = 0 to activeList.Count - 1 do
       
                     let a : int = int (successorList.Item(activeList.Item(i)))
                     let mutable next = -1
                     if i + 1 >= activeList.Count then next <- 0 else next <- i + 1
                     if activeList.Item(next) <> a then
                          successorList.Remove(activeList.Item(i)) |> ignore
                          successorList.Add(activeList.Item(i), int(activeList.Item(i + 1)))


           | Acknowledgement ->
                    printfn("All nodes are joined")
                    (select ("akka://FSharp/user/master") system) <! RouteFinish(3)


            | JoinNode(id) ->
                  refNode<- activeList.Item(0)
                  predecessorList.Add(id,-1)
                  successorList.Add(id,temp)
                  activeList.Add(id)
                  (select ("akka://FSharp/user/master") system) <! RouteFinish(3)
                  //sender <!Acknowledgement

            | StartJoin(i,id) ->
                  //printfn("\nnode join")
                  if activeList.Count=0 then 
                    activeList.Add(id)
                    successorList.Add(id,id)
                    predecessorList.Add(id,id)
                  if activeList.Count=1 then
                    activeList.Add(id)
                    successorList.Add(id,activeList.Item(0))
                    predecessorList.Add(id,activeList.Item(0))
                    successorList.Remove(activeList.Item(0))|>ignore
                    successorList.Add(activeList.Item(0),id)
                    predecessorList.Remove(activeList.Item(0))|>ignore
                    predecessorList.Add(activeList.Item(0),id)
                    tempList.Add(activeList.Item(0))
               
                  if i=inputNodes then
                    (select ("akka://FSharp/user/master") system) <! RouteFinish(id)
                    
            | _ -> failwith "Message is not expected!"


        return! loop()
    }
    loop()


let MasterNode(mailbox: Actor<_>) =
    
    let firstGroup = Array.copy randomMapping.[0..inputNodes-1]
 
    for i in 0..inputNodes-1 do
        spawn mailbox (string randomMapping.[i]) (childNode randomMapping.[i]) |> ignore  // Supervisor strategy

    let rec loop() = actor{
        let! message = mailbox.Receive()
        match message with
        | StartJoining -> 
            for id in firstGroup do
                if activeList.Contains(id) then
                   printfn(" ")
                else
                   let childNodeRef = selectActor id
                   //printfn("ID= %i")id
                   i<-i+1
                   childNodeRef <!StartJoin(i,id)
                

        | RouteFinish(hops) -> 
                printfn("\n The average number of hops traversed = %f")ihops
                printfn("\n Time = %f ms")time
                system.Stop(mailbox.Self)
                endFlag <- true

        | _-> failwith "Unknown message received!"

        return! loop()
    }
    loop()


let masterRef = spawn system "master" (MasterNode)

masterRef <! StartJoining

while not endFlag do
    ignore()

printfn" "