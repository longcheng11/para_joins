import x10.io.File;
import x10.util.ArrayList;
import x10.util.List;
import x10.array.Array;
import x10.util.HashMap;
import x10.io.ReaderIterator;
import x10.util.Map.Entry;
import x10.util.Random;
import x10.util.concurrent.AtomicBoolean;
import x10.util.concurrent.AtomicLong;
import x10.util.concurrent.AtomicInteger;
import x10.compiler.Native;
import x10.compiler.NativeCPPInclude;
import x10.compiler.NativeCPPCompilationUnit;
import x10.util.StringBuilder;
import x10.util.concurrent.AtomicInteger;
import x10.util.HashSet;

@NativeCPPInclude("gzRead.h")
@NativeCPPCompilationUnit("gzRead.cc")

//mapping according to the M axis
public class PRPQ_plus {
	
	@Native("c++","gzRead(#1->c_str())")
	static native def gzRead(file:String):String;
	
	public static class Pair {
		
		private var key:Long;
		private var payload:Long;
		
		public def this(){
			this.key=0L;			
			this.payload=0L;		
		}
		
		public def this(k:Long,p:Long){
			this.key=k;			
			this.payload=p;		
		}
		
		public def getK():Long{
			return key;
		}
		
		public def getP():Long{
			return payload;
		}
		
	}
	
	public static def Serialize(A:Array[String],B:Array[Char]){
		var size:Int=A.size;
		var num1:Int=0;
		for (i in (0..(size-1))){
			val b=A(i);
			val c=b.length();
			for(j in (0..(c-1))){
				B(num1)=b(j);
				num1++;
			}
			B(num1)='\n';
			num1++;
		}
	}
	
	public static def DeSerialize(A:RemoteArray[Char],B:Array[String]){
		var size:Int=A.size;
		var tmp:StringBuilder=new StringBuilder();
		var num1:Int=0;
		for(i in (0..(size-1))){
			if(A(i)!='\n') {
				tmp.add(A(i));
			}
			else {
				B(num1)=tmp.toString();
				num1++;
				tmp=new StringBuilder();
			}	  	
		}
		tmp=null;
	}
	
	public static def toByte(x:Long):Array[Byte]{
		var tb:Array[Byte]=new Array[Byte](8);
		tb(7) = (x >> 56) as Byte;
		tb(6) = (x >> 48) as Byte;
		tb(5) = (x >> 40) as Byte;
		tb(4) = (x >> 36) as Byte;
		tb(3) = (x >> 24) as Byte;
		tb(2) = (x >> 16) as Byte;
		tb(1) = (x >> 8) as Byte;
		tb(0) = (x >> 0) as Byte;
		return tb;
	}
	
	//Parsing Realtions
	public static def Parsing(line:String):Array[Long]{ 		
		var value:Array[Long]=new Array[Long](2);
		
		try{
			//R
			var r:String=line.substring(0,line.indexOf('|')); 
			value(0)=Long.parse(r);
			
			//S
			var s:String=line.substring(r.length()+1);
			value(1)=Long.parse(s);
		}
		catch (Exception){
			value(0)=-1L;
			value(1)=-1L;
		}	
		
		return value;
	} 
	
	public static def hash_3(key:Long,size:Int):Int {
		var s:Long=size as Long;
		var mod:long=key%s;	
		return mod as Int;
	} 
	
	public static def ArraySort(a:Array[Pair]){
		val s:Int=a.size;
		var temp:Pair;
		var i:Int,j:Int;
		for (i=1;i<s;i++) {
			for (j=i;j>0;j--) {  
				if (a(j).getP()<a(j-1).getP()) {  
					temp=a(j);  
					a(j)=a(j-1);  
					a(j-1)= temp;  
				}  
			}  
		} 
	}
	
	public static def main(args: Array[String]) {
		// TODO auto-generated stub
		
		val N:Int=Place.MAX_PLACES;
		val FILE=Int.parse(args(0));
		val RATE=Int.parse(args(1));
		val THRES=Int.parse(args(2));
		Console.OUT.println("<#places> "+N+" <chunk/thread> "+FILE+" <sample rate> "+RATE+" <threshold> "+THRES);
		val path_r=args(3);
		val path_s=args(4);
		
		val region:Region=0..(N-1);
		val d:Dist=Dist.makeBlock(region);
		
		/**initialize the Dictionary Tables on each place*/
		//read
		val R_list=DistArray.make[ArrayList[Pair]](d);
		val S_list=DistArray.make[ArrayList[Pair]](d);
		
		//h local kept
		val h_local=DistArray.make[Array[HashMap[Long,ArrayList[Long]]]](d);
		
		//h query
		val h_key_collector=DistArray.make[Array[Array[Long]]](d);
		val h_keys_query=DistArray.make[Array[RemoteArray[Long]]](d);
		val h_payload_return=DistArray.make[Array[RemoteArray[Long]]](d);	
		
		//remote receive
		val R_keys_receive=DistArray.make[Array[RemoteArray[Long]]](d);	
		val R_payload_receive=DistArray.make[Array[RemoteArray[Long]]](d);
		val S_keys_receive=DistArray.make[Array[RemoteArray[Long]]](d);	
		val S_payload_receive=DistArray.make[Array[RemoteArray[Long]]](d);
		
		//R hash table
		val R_table=DistArray.make[HashMap[Long,Long]](d);
		
		//counter for received R
		val R_counters=DistArray.make[AtomicInteger](d);
		
		//counter for join
		val join_counters=DistArray.make[Array[Int]](d);
		
		//initialize the object at each place		
		finish for (p in Place.places()){
			at (p) async {
				//read
				R_list(here.id)=new ArrayList[Pair]();
				S_list(here.id)=new ArrayList[Pair]();
				
				//h local kept
				h_local(here.id)=new Array[HashMap[Long,ArrayList[Long]]](N);
				
				//h query	
				h_key_collector(here.id)=new Array[Array[Long]](N);
				h_keys_query(here.id)=new Array[RemoteArray[Long]](N);
				h_payload_return(here.id)=new Array[RemoteArray[Long]](N);
				
				//receive -  the remote arrays
				R_keys_receive(here.id)=new Array[RemoteArray[Long]](N);
				R_payload_receive(here.id)=new Array[RemoteArray[Long]](N);
				S_keys_receive(here.id)=new Array[RemoteArray[Long]](N);
				S_payload_receive(here.id)=new Array[RemoteArray[Long]](N);
				
				//R hash table
				R_table(here.id)=new HashMap[Long,Long]();
				
				//counter for received R
				R_counters(here.id)=new AtomicInteger(0);		
				
				//counter for join
				join_counters(here.id)=new Array[Int](6);	
			}
		}
		
		//Reading items in memory - arraylist
		var read_start:Long=System.currentTimeMillis();	
		
		finish for (p in Place.places()){
			at (p) async {		
				val f_start:Int=here.id*FILE;
				val f_end:Int=(here.id+1)*FILE;
				for(var e3:Int=f_start;e3<f_end;e3++){
					
					//read R
					var R_file:String=path_r+e3.toString()+".long.gz";
					//Console.OUT.println("place "+here.id+ " thread "+e3+" file "+R_file);
					var temp_r:File=new File(R_file);
					if(temp_r.exists()){
						var lstring:String=gzRead(R_file);
						var len:Int=lstring.length();
						var start:Int=0;
						var end:Int=0;
						var line:String;						
						var value:Array[Long]=new Array[Long](2);
						var R:Pair;
						while(start<len) {
							end=lstring.indexOf('\n',start);
							line=lstring.substring(start,end);
							value=Parsing(line);
							if(value(0)!=-1L) {
								R=new Pair(value(0),value(1));
								R_list(here.id).add(R);
							}
							start=end+1;							
						}	
					}	 //end if	
					
					//read S
					var S_file:String=path_s+e3.toString()+".long.gz";
					//Console.OUT.println("place "+here.id+ " thread "+e3+" file "+S_file);
					var temp_s:File=new File(S_file);
					if(temp_s.exists()){
						var lstring:String=gzRead(S_file);
						var len:Int=lstring.length();
						var start:Int=0;
						var end:Int=0;
						var line:String;
						var value:Array[Long]=new Array[Long](2);
						var S:Pair;
						while(start<len) {
							end=lstring.indexOf('\n',start);
							line=lstring.substring(start,end);
							value=Parsing(line);
							S=new Pair(value(0),value(1));
							S_list(here.id).add(S);
							start=end+1;							
						}	
					}	 //end if						
				} //end for e3
				//Console.OUT.println("place "+here.id+" R is "+R_list(here.id).size()+" S is "+S_list(here.id).size());		
				System.gc();
			} //end async at place 
		} //end finish place
		var read_end:Long=System.currentTimeMillis();
		Console.OUT.println("Read time is "+(read_end-read_start)+" ms");	
		System.gc();
		
		//R distribution
		var dis_start_r:Long=System.currentTimeMillis();	
		finish for( p in Place.places()){
			at (p) async {
				val pn:Int=here.id;					
				var R_key_collector:Array[ArrayList[Long]]=new Array[ArrayList[Long]](N);
				var R_payload_collector:Array[ArrayList[Long]]=new Array[ArrayList[Long]](N);
				for(j in (0..(N-1))){
					R_key_collector(j)=new ArrayList[Long]();
					R_payload_collector(j)=new ArrayList[Long]();
				}
				
				//hash distribution
				var des:Int;
				for(r in R_list(here.id)){
					des=hash_3(r.getK(),N);
					R_key_collector(des).add(r.getK());
					R_payload_collector(des).add(r.getP());
				}
				
				//push the R to remote places
				var keys_array:Array[long];
				var payload_array:Array[long];
				for( i in (0..(N-1))) {
					var k:int=(pn+i)%N;
					keys_array=R_key_collector(k).toArray();
					payload_array=R_payload_collector(k).toArray();
					val kk=k;
					val pk=Place.place(k);
					val s1=keys_array.size;	
					if(pn==k){
						R_keys_receive(here.id)(pn)= new RemoteArray(keys_array);
						R_payload_receive(here.id)(pn)= new RemoteArray(payload_array);
						R_counters(here.id).addAndGet(s1);
					}
					else{
						at(pk){
							R_keys_receive(here.id)(pn)= new RemoteArray(new Array[Long](s1));
							R_payload_receive(here.id)(pn)= new RemoteArray(new Array[Long](s1));
							R_counters(here.id).addAndGet(s1);
						}
						Array.asyncCopy( keys_array, at (pk) R_keys_receive(here.id)(pn));
						Array.asyncCopy( payload_array, at (pk) R_payload_receive(here.id)(pn));
					}
				}  //end pushing	
				
				//empty the read in R
				R_list(here.id)=null;
				
			} //end async at place
		} 
		
		var dis_end_r:Long=System.currentTimeMillis();	
		Console.OUT.println("R Distribution Takes "+(dis_end_r-dis_start_r)+" ms");
		
		// S sample and distribution
		finish for( p in Place.places()){
			at (p) async {
				//pre to process the Relation S and h
				val pn:Int=here.id;					
				var S_key_collector:Array[ArrayList[Long]]=new Array[ArrayList[Long]](N);
				var S_payload_collector:Array[ArrayList[Long]]=new Array[ArrayList[Long]](N);	
				var h_key_local:Array[ArrayList[Long]]=new Array[ArrayList[Long]](N);			
				
				
				for(i in (0..(N-1))){
					S_key_collector(i)=new ArrayList[Long]();
					S_payload_collector(i)=new ArrayList[Long]();
					h_key_local(i)=new ArrayList[Long]();
				}
				
				var s_size:Int=S_list(here.id).size();
				
				//catch the high skew keys of S 
				var sample_set:HashSet[Long]=new HashSet[Long]();
				
				//a hash table to counter the sampled keys, 10% of the whole size of S
				var s1:Int=s_size/100*RATE;
				var h_counter:HashMap[Long,int]=new HashMap[Long,int](s1);
				
				//random sample
				var random_start:Long=System.currentTimeMillis();	
				var rn:Random=new Random();
				var rn_i:Int;
				var key_1:Long;
				for(i in 0..(s1-1)){
					rn_i=(rn.nextDouble()*s_size) as Int;
					key_1=S_list(here.id)(rn_i).getK();
					var num:Int;
					if(h_counter.containsKey(key_1)){
						num=h_counter.get(key_1).value+1;
						h_counter.put(key_1,num);
					}
					else{
						h_counter.put(key_1,1);
					}
				}
				var random_en:Long=System.currentTimeMillis();	
				
				//pick up the skew keys, put them in the sample HashSet and also keep them in the Tobe sent query keys collector 
				var s2:Int=h_counter.size();
				val iter=h_counter.entries().iterator();
				var des:Int;
				var key_a:Long;
				while(iter.hasNext()){
					val entry=iter.next();
					if(entry.getValue()>THRES){
						key_a=entry.getKey();
						des=hash_3(key_a,N);
						h_key_local(des).add(key_a);
						sample_set.add(key_a);
					}
				}
				var pick_end:Long=System.currentTimeMillis();
				//Console.OUT.println("<random takes> "+(random_en-random_start)+" <pick takes> "+(pick_end-random_en)+" <pick size> "+sample_set.size());
				join_counters(here.id)(5)+=sample_set.size();
				
				//push high-skew keys to Remote Places
				for(j in 0..(N-1)){
					var i:int=(pn+j)%N;
					val ii=i;
					val pi=Place.place(i);
					val s21=h_key_local(i).size();
					h_payload_return(here.id)(i)=new RemoteArray(new Array[Long](s21)); //initialize the size of the received values
					h_key_collector(here.id)(i)=h_key_local(i).toArray();
					at(pi){
						h_keys_query(here.id)(pn)=new RemoteArray(new Array[Long](s21));
					}
					Array.asyncCopy( h_key_collector(here.id)(i), at (pi) h_keys_query(here.id)(pn));
				} //end push
				
				//1->n structure for h
				for(i in 0..(N-1)){
					h_local(here.id)(i)=new HashMap[Long,ArrayList[Long]]();
				}
				
				//start to process the Raltion S
				var key:Long;
				var value:Long;
				for(s in S_list(here.id)){					
					key=s.getK();
					value=s.getP();
					
					//the high skew tuples
					if(sample_set.contains(key)){
						des=hash_3(key,N);
						if(h_local(here.id)(des).containsKey(key)){
							h_local(here.id)(des).get(key).value.add(value);
						}
						else{
							h_local(here.id)(des).put(key,new ArrayList[Long]());
							h_local(here.id)(des).get(key).value.add(value);
						}
					}
					
					//the not-high skew tuples
					else{						
						des=hash_3(key,N);
						S_key_collector(des).add(key);
						S_payload_collector(des).add(value);
					}
				}
				
				//push the S to remote places
				var keys_array:Array[long];
				var payload_array:Array[long];
				for( i in (0..(N-1))) {
					var k:int=(pn+i)%N;
					keys_array=S_key_collector(k).toArray();
					payload_array=S_payload_collector(k).toArray();
					val kk=k;
					val pk=Place.place(k);
					val s3=keys_array.size;	
					if(pn==k){
						S_keys_receive(here.id)(pn)= new RemoteArray(keys_array);
						S_payload_receive(here.id)(pn)= new RemoteArray(payload_array);
					}
					else{
						at(pk){
							S_keys_receive(here.id)(pn)= new RemoteArray(new Array[Long](s3));
							S_payload_receive(here.id)(pn)= new RemoteArray(new Array[Long](s3));
						}
						Array.asyncCopy( keys_array, at (pk) S_keys_receive(here.id)(pn));
						Array.asyncCopy( payload_array, at (pk) S_payload_receive(here.id)(pn));
					}
				}  //end pushing S
				
				//empty the read in S
				S_list(here.id)=null;
				
			} //end async at place
		}	
		var dis_end_s:Long=System.currentTimeMillis();	
		Console.OUT.println("S Distribution Takes "+(dis_end_s-dis_end_r)+" ms");
		
		//build R hash tables, Join with S and Query for h
		finish for( p in Place.places()){
			at (p) async {
				val pn:Int=here.id;	
				
				//build R hash table
				val s0:Int=R_counters(here.id).get();
				var r_hash_table:HashMap[Long,Long]=new HashMap[Long,Long](s0);
				for(i in 0..(N-1)){
					var s1:Int=R_keys_receive(here.id)(i).size;
					for(var j:Int=0;j<s1;j++){
						r_hash_table.put(R_keys_receive(here.id)(i)(j),R_payload_receive(here.id)(i)(j));
					}
				}
				
				//check S
				for(i in 0..(N-1)){
					var s2:Int=S_keys_receive(here.id)(i).size; 
					join_counters(here.id)(2)+=s2;
					for(var j:Int=0;j<s2;j++){
						if(r_hash_table.containsKey(S_keys_receive(here.id)(i)(j))){
							join_counters(here.id)(0)+=1;
						}
						else{
							join_counters(here.id)(1)+=1;
						}
					}					
				}
				
				//return the query of h
				var h_payload_collector:Array[Array[Long]]=new Array[Array[Long]](N);
				
				for(x in 0..(N-1)){
					var i:int=(pn+x)%N;
					var s3:Int=h_keys_query(here.id)(i).size;
					h_payload_collector(i)=new Array[Long](s3);
					join_counters(here.id)(4)+=s3;
					for(var j:Int=0;j<s3;j++){					
						if(r_hash_table.containsKey(h_keys_query(here.id)(i)(j))){
							h_payload_collector(i)(j)=r_hash_table.get(h_keys_query(here.id)(i)(j)).value;
						}
						else{
							h_payload_collector(i)(j)=0L;
						}
					}
					val pi=Place.place(i);
					//val s4=h_payload_collector(i).size;
					//Console.OUT.println("Return h: "+pn+"->"+i+" <size> "+s4);
					Array.asyncCopy( h_payload_collector(i), at (pi) h_payload_return(here.id)(pn));
				}
				
			} //end async at place
		}
		var join_end_s:Long=System.currentTimeMillis();	
		Console.OUT.println("S Join Takes "+(join_end_s-dis_end_s)+" ms");
		
		//final join with h by returned payload
		finish for( p in Place.places()){
			at (p) async {
				
				var s1:Int;
				var join_key:Long;
				for(i in 0..(N-1)){
					s1=h_payload_return(here.id)(i).size;
					for(var j:Int=0;j<s1;j++){
						if(h_payload_return(here.id)(i)(j)!=0L){
							join_key=h_key_collector(here.id)(i)(j);
							join_counters(here.id)(0)+=h_local(here.id)(i).get(join_key).value.size();
						}
						else{
							join_counters(here.id)(1)+=1;
						}
					}
				}
				
			} //end async at place
		}
		var join_end_h:Long=System.currentTimeMillis();	
		Console.OUT.println("h Join Takes "+(join_end_h-join_end_s)+" ms");
		
		Console.OUT.println("*****************************");
		Console.OUT.println("The Whole Time is"+(join_end_h-read_end)+" ms");
		
		finish for( p in Place.places()){
			at (p) {
				Console.OUT.println("Place: "+here.id+" "+join_counters(here.id)(0)+" "+join_counters(here.id)(1)+" "+join_counters(here.id)(2)+" "+join_counters(here.id)(3)+" "+join_counters(here.id)(4)+" "+join_counters(here.id)(5));
			}
		}		
	}
}

