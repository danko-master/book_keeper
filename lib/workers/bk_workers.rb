require 'bson'
require 'bunny'

module BkWorkers
  class DebitKredit
    include Sidekiq::Worker
    sidekiq_options queue: :bk

    def perform(num_inst)
      @experiment_logger = []

      @bunny = Bunny.new
      @current_logger = Logger.new("#{File.dirname(__FILE__)}/../../log/sidekiq_#{ENV['APP_ENV']}_inst_#{num_inst}.log")
      @current_logger.info "NOTIFICATIONS: Started"      
      begin
        @current_logger.info p " [*] RUBY Waiting for messages. To exit press CTRL+C"
        @bunny.start
        @ch   = @bunny.create_channel
        run
      rescue Interrupt => _
        @bunny.close
        @current_logger.info "NOTIFICATIONS: Stopped"
        exit(0)
      end
    end

    def run     
      @current_logger.info p "Выполняем run, ждем tdr."  
      q    = @ch.queue($config['runner']['input_queue']) 
      q.subscribe(:block => true, :manual_ack => true) do |delivery_info, properties, body|
        time1 = Time.now

        tdr_data = Hash.new
        tdr_data['delivery_tag'] = delivery_info.delivery_tag
        tdr_data['tdr'] = body 
        @current_logger.info p "Bunny ::: получили данные #{tdr_data}"


        if tdr_data.present?
          tdr = eval( tdr_data['tdr'] )
          @current_logger.info p "Новый tdr #{tdr}"
          p debit = tdr["sum"].to_f

          company = Db::Company.find_by_id(tdr["customer_id"])
          if company.present?
            p kredit = company.balance.to_f
            
            alpha = debit - kredit
            if alpha > -100
              p "превышен порог"
              p "ставим флаг"
            else
              p "все нормально"
            end

            send_tdr_data_to_rabbit(tdr, alpha)
          end

          delivery_tag = tdr_data['delivery_tag']
          # отправка ack в канал
          @current_logger.info p "Отправка ack в RabbitMQ ::: delivery_tag: #{delivery_tag}"
          @ch.ack(delivery_tag)

          @current_logger.info p "Обработан tdr #{tdr} ::: debit-kredit #{alpha}"  
        end

        time2 = Time.now
        @experiment_logger << (time2 - time1)
        if @experiment_logger.size >= 9900
          m = @experiment_logger
          @current_logger.info p "Среднее время выполнения"
          p (m.inject(0){ |sum,el| sum + el }.to_f)/ m.size
        end
      end
    end

    # информация из TDR-тарифов
    # def get_tdr_data_from_rabbit
    #   q    = @ch.queue($config['runner']['input_queue'])   
    #   tdr_data = nil
    #   q.subscribe(:manual_ack => true) do |delivery_info, properties, body|
    #     tdr_data = Hash.new
        
    #     tdr_data['delivery_tag'] = delivery_info.delivery_tag
    #     tdr_data['tdr'] = body

    #     @current_logger.info p "Bunny ::: получили данные #{tdr_data}"
    #   end     

    #   # @current_logger.info p "Bunny ::: recieve data #{tdr_data}"
    #   tdr_data
    # end

    def send_tdr_data_to_rabbit(tdr, alpha)
      @current_logger.info p "Отправка tdr в RabbitMQ #{tdr} ::: debit-kredit: #{alpha}"
      q    = @ch.queue($config['runner']['output_queue'])

      tdr_bson = BSON::Document.new(
        # id машины
        imei: tdr['imei'], 
        road_id: tdr['road_id'], 
        lat0: tdr['lat0'], 
        lon0: tdr['lon0'], 
        time0: tdr['time0'], 
        lat1: tdr['lat1'], 
        lon1: tdr['lon1'], 
        time1: tdr['time1'], 
        path: tdr['path'],
        sum: tdr['sum'],
        customer_id: ['customer_id'],
        debit_kredit: alpha
      )

      @ch.default_exchange.publish(tdr_bson.to_s, :routing_key => q.name)
    end
  end
end