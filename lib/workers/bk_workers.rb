require 'bson'
require 'bunny'

module BkWorkers
  class DebitKredit
    include Sidekiq::Worker
    sidekiq_options queue: :bk

    def perform(num_inst)
      #loop do
        @current_logger = Logger.new("#{File.dirname(__FILE__)}/../../log/sidekiq_#{ENV['APP_ENV']}_inst_#{num_inst}.log")
        @current_logger.info { "NOTIFICATIONS: Started" }
        @bunny = Bunny.new
        @bunny.start
        @ch   = @bunny.create_channel
        run
        @bunny.close
      #end
    end

    def run     
      tdr_data = get_tdr_data_from_rabbit
      if tdr_data.present?
        tdr = eval( tdr_data['tdr'] )
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
      end
    end

    # информация из TDR-тарифов
    def get_tdr_data_from_rabbit
      q    = @ch.queue($config['runner']['input_queue'])   
      tdr_data = nil
      q.subscribe(:manual_ack => true) do |delivery_info, properties, body|
        tdr_data = Hash.new
        
        tdr_data['delivery_tag'] = delivery_info.delivery_tag
        tdr_data['tdr'] = body

        @current_logger.info p "Bunny ::: получили данные #{tdr_data}"
      end     

      # @current_logger.info p "Bunny ::: recieve data #{tdr_data}"
      tdr_data
    end

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