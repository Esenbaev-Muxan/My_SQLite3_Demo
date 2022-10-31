time = Time.new
puts "MySQLite version 0.1 #{time.strftime("%Y/%d/%m")}"
require_relative "req.rb"

class CLI_Interface
    @@csvr = MySqliteRequest.new

    def initialize()
        @cli_input = "" 
        @input = nil    
        @table = ''
    end
    
    def get_input()


        @cli_input = gets.chomp

        if @cli_input == "exit" 


            puts "exiting programing..."
            return
        end





        @input = @cli_input.split
    
        case @input[0]


        when "SELECT"  
            if (@cli_input.include? "ORDER BY")
                cli_order()

            end
            if (@cli_input.include? "WHERE")
                cli_where()
            end
            if (@cli_input.include? "JOIN")
                cli_join()

            end
            cli_select()
        when "INSERT"
            cli_insert()
        when "UPDATE"
            cli_where()
            cli_update()
        when "DELETE"
            cli_where()
            cli_delete()
        else

            puts "Invalid inputs."
        end
    end

    def cli_select()


        column = @cli_input.split("FROM").first.tr("SELECT ", "").split(",")
        @table = @cli_input.split("FROM").last.tr!(" ", "")
    
    
        if (!@cli_input.include? "FROM" || !table || (column.include? " ") )

            raise "Invalid Syntax \n\tSYNTAX: SELECT column1, column2 FROM table"
            get_input()
        end
     

        @@csvr.from(@table).select(column).run
        get_input()
    end

    def cli_where() 
    
        @cli_input, where = @cli_input.split(" WHERE ")
        column, criteria = where.split(" = ")

        if Integer(criteria, exception:false) 
            criteria = criteria.to_i
        else

            criteria.tr!("';", "")
            criteria.tr!('"', "")
        end
        @@csvr.where(column,criteria)
    end
    
    def cli_insert()
       
        table = @input[2]            #
        values = @cli_input.split("VALUES").last 
        values.tr!("();", "").slice!(0)

      



        arr = values.split(/(".*?"|[^",\s]+)(?=\s*,|\s*$)/).reject{|elem| elem == ', ' || elem == " " || elem == "" || elem.empty?}
  
        if (@input[1] != "INTO" || !table || @input[3] != "VALUES" || values.size == 0)
            puts "Invalid Syntax:\n\tSYNTAX: INSERT INTO `table` VALUES (column1, column2, column3, ...)"
            get_input()
        end
        
        @@csvr.insert(table)
        newhash = {}

       




        (0...arr.size).each do |i| 
            arr[i].gsub!('"',"")
            if Integer(arr[i], exception:false).nil?
                newhash[@@csvr.headers[i]] = arr[i] 
                next
            end
            newhash[@@csvr.headers[i]] = arr[i].to_i 
        end

        
        @@csvr.values(newhash).run

        get_input()
    end

   


    def match_symbol_to_data(arr)
        hash = {}
        arr.each_with_index do |elements, index|
            if index.even?
                elements.tr!(",= ","")
                if Integer(arr[index+1], exception: false).nil? 
                    hash[arr[index].to_sym] = arr[index+1].gsub('"',"")
                else
                    hash[arr[index].to_sym] = arr[index+1].gsub('"',"").to_i
                end
            end
        end
        return hash

    end

    def cli_update()
        


     


        table = @input[1]

       
        if (!table || @input[2] != "SET")

            puts "Invalid Syntax \n\tSYNTAX: FROM table SET column = value WHERE column2 = value2"
            get_input()
        end
    
  
        set_values = @cli_input.split(" SET ").last         #
        
     
        arr1 = set_values.split(/(".*?"|[^",\s]+)(?=\s*,|\s*$)/).reject{|elem| elem == ', ' || elem == " " || elem == "" || elem.empty?}



        hash = match_symbol_to_data(arr1)
        
        @@csvr.update(table).set(hash).run
        get_input
    end
    
    def cli_delete()
        table = @input[2]
        if (@input[1] != "FROM" || !table)

            puts "Invalid syntax_1"
            get_input()
        end
        @@csvr.delete.from(table).run
        get_input()
    end

    def cli_join()
    

    @cli_input, join_query = @cli_input.split(" JOIN ")
    table_to_join, data_to_join = join_query.split(" ON ")
    table_to_join.tr!(" ","")
    data_to_join = data_to_join.split(" = ")

 
    @@csvr.join(data_to_join[0],table_to_join,data_to_join[1])
    end

    def cli_order()
        @cli_input, parsed = @cli_input.split("  ORDER BY  ")



        column, order = parsed.split(" ")
        @@csvr.order(column,order)
    end
end

interface = CLI_Interface.new

interface.get_input

