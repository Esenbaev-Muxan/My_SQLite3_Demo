require 'csv'

class MySqliteRequest
    attr_accessor :headers

    def initialize()
        @headers = nil # contains the headers of the csv file
        @table_name = nil # contains the path to the table
        @table = nil 
        @select = []
        @where_column = ''
        @where_criteria = ''
        @join_column_a = ''
        @db_b = ''
        @join_column_b = ''
        @select_results = []
        @values = []
        @query_type = nil
        @set_data = []
        @set_column = []
        @where_results = []
        @order = ""
        @order_column = ""
        @where_flag = false
        @columns = nil
    end

    def query_checker(querty)
        raise "Too many query types" unless @query_type == querty
    end


    def table_builder(paths)
        if !paths.end_with? ".csv" 
            paths << ".csv"
        end
        table = []
        headers = nil
        CSV.foreach(paths, headers: true ,header_converters: :symbol) do |hash| 
            hash.each do |key, val|
                if Integer(val, exception:false).nil?
                    next
                end
                hash[key] = val.to_i
            end
            headers ||= hash.headers
            table << hash.to_h 
        end
        return [table,headers]
    end

    def from(table_name) 
        if !table_name.end_with? ".csv"
            table_name << ".csv"
        end
        @table_name = table_name
        @table, @headers = table_builder(@table_name)
        self
    end

    def select(*columns)
        @columns = columns
        if columns.class == Array

            columns = columns.flatten(1)
        end
        @select = columns
        if columns[0] == "*"
            @select = @headers.map{ |x| x.to_s}
        end
        @query_type ||= "select"
        query_checker("select")
        self
    end

    def run_where()
        @table.compact!
        @table.each do |hash|
            if hash == nil
                next
            end
            if hash[@where_column.to_sym] == @where_criteria
                @where_results.push(hash)
            end
        end
        @where_flag = true
    end

    def where(column, criteria) 
        @where_column = column
        @where_criteria = criteria
        self
    end

    def order(column_name,order) 
        @order = order
        @order_column = column_name
        self
    end

    def run_order()
        @select_results = @select_results.sort_by{|hsh| hsh[@order_column.to_sym]}
        if @order.downcase == "desc"
            @select_results.reverse!
        end
    end

    def join(column_on_a, filename_db_b, columname_on_db_b) 
        @join_column_a = column_on_a
        @db_b = filename_db_b
        @join_column_b = columname_on_db_b
        self
    end

    def run_join()
    
        csv1, headers1 = table_builder(@table_name)
        csv2, headers2 = table_builder(@db_b) 

        joined_table = []
        @headers = headers1 + headers2

            csv1.each_with_index do |hash, i|
                csv2.each_with_index do |hashb, j|
                    if csv1[i][@join_column_a.to_sym] == csv2[j][@join_column_b.to_sym]
                        joined_table[i] = csv1[i].merge(csv2[j])
                    end

                end
            end
        if @columns[0] == "*"
            @select = @headers.map{ |x| x.to_s}
        end
        
        @table = joined_table
        if @where_flag
            run_where
        end
        self
    end

    def insert(table_name)

        @table_name = table_name
        @table, @headers = table_builder(@table_name)
        @query_type ||= "insert"
        query_checker("insert")
        puts "Record Inserted!"
        self
    end

    def values(data)
    


        raise "keys from data don't match headers" unless data.size == @headers.size and (data.keys - @headers).empty?
        @table.push(data)
        self
    end

    def update(table_name)
        @table_name = table_name
        @table, @headers = table_builder(table_name)
        @query_type ||= "update"
        query_checker("update")
        puts "Updated"
        self
    end

    def set(data)
        @set_data = data
        self
    end
        
    def run()
        case @query_type
        when "select"
            if @db_b != ""
                run_join
            end
            if @where_column != '' 
            
                run_where
            end
            run_select
        when "insert"
            update_table
        when "update"
            run_update
        when "delete"
            run_delete
        end
        reset()
    end

    def reset()
        @headers = nil # contains the headers of the csv file
        @table_name = nil # contains the path to the table
        @table = nil # contains the parsed CSV file from the CSV gem
        @select = []
        @where_column = ''
        @where_criteria = ''
        @join_column_a = ''
        @db_b = ''
        @join_column_b = ''
        @select_results = []
        @values = []
        @query_type = nil
        @set_data = []
        @set_column = []
        @where_results = []
        @order = ""
        @order_column = ""
        @where_flag = false
        @columns = nil
    end


    def run_select()
        if @where_results.size != 0 && @where_flag 
            @table = @where_results
        end
        if @where_results.size == 0 && @where_flag
            puts "No results found! Check your where statement"
            return
        end


        @table.each_with_index do |hash|
            if hash == nil
                next
            end
            newhash = {} 
            @select.each do |column| 
                newhash[column.to_sym] = hash[column.to_sym]
            end
            @select_results.push(newhash)
        end

        if @order
            run_order
        end

        @select_results = @select_results.uniq

        @select_results.each_with_index do |hash, index| 
            output = ''
            hash.each do |key, value|
                output += value.to_s + "|"
            end
            output.chop!
            print output
            puts
        end

    end

    def update_table()
        temp_header_array = []
        @headers.each_with_index do |element, index| 
            temp_header_array.push(@headers[index].to_s)
        end
        CSV.open(@table_name, "w+") do |csv|
            csv << temp_header_array
                @table.each do |hash|
                csv << hash.values
            end
        end
    end

    def run_update()

        @table.each do |hash| 
            if hash[@where_column.to_sym] == @where_criteria 
                @set_data.each do |key, value| 
                    hash[key] = value
                end
            end
        end

       update_table()  
    end

    def run_delete()
    
        @table.each do |element|
            if (element[@where_column.to_sym] == @where_criteria)
                
                @table.delete(element)
                puts "Record Deleted!"
            end
        end
        update_table()
    end

    def delete()
        @query_type = "delete"
        query_checker("delete")
        self
    end
end

