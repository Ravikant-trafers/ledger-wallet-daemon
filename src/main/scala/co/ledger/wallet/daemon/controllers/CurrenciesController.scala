package co.ledger.wallet.daemon.controllers

import co.ledger.wallet.daemon.async.MDCPropagatingExecutionContext.Implicits.global
import co.ledger.wallet.daemon.controllers.requests.{CommonMethodValidations, RequestWithUser, WithPoolInfo}
import co.ledger.wallet.daemon.controllers.responses.ResponseSerializer
import co.ledger.wallet.daemon.services.CurrenciesService
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finatra.request.{QueryParam, RouteParam}
import com.twitter.finatra.validation.{MethodValidation, ValidationResult}
import javax.inject.Inject

import scala.concurrent.Future

class CurrenciesController @Inject()(currenciesService: CurrenciesService) extends WDController {

  import CurrenciesController._

  /**
    * End point queries for currency view with specified name. The name is unique and follows the
    * convention <a href="https://en.wikipedia.org/wiki/List_of_cryptocurrencies">List of cryptocurrencies</a>.
    * Name should be lowercase and predefined by core library.
    *
    */
  deprecated(get[GetCurrencyRequest, Future[Response]], "/pools/:pool_name/currencies/:currency_name", callback =
    { request: GetCurrencyRequest =>
    val currencyName = request.currency_name
    info(s"GET currency $request")
    currenciesService.currency(currencyName, request.poolInfo).map {
      case Some(currency) => ResponseSerializer.serializeOk(currency, response)
      case None => ResponseSerializer.serializeNotFound(
        Map("response" -> "Currency not support", "currency_name" -> currencyName), response)
    }
  })

  /**
    * End point queries for currencies view belongs to pool specified by pool name.
    *
    */
  get("/pools/:pool_name/currencies")  { request: GetCurrenciesRequest =>
    info(s"GET currencies $request")
    currenciesService.currencies(request.poolInfo)
  }

  /**
    * Endpoint validating the given address. Response boolean value true if is valid,
    * false otherwise.
    *
    */
  get("/pools/:pool_name/currencies/:currency_name/validate") { request: AddressValidatingRequest =>
      currenciesService.validateAddress(request.address, request.currency_name, request.poolInfo)
  }

}

object CurrenciesController {

  case class AddressValidatingRequest(@RouteParam pool_name: String,
                                      @RouteParam currency_name: String,
                                      @QueryParam address: String,
                                      request: Request) extends RequestWithUser with WithPoolInfo

  case class GetCurrenciesRequest(
                                   @RouteParam pool_name: String,
                                   request: Request
                                 ) extends RequestWithUser with WithPoolInfo {
    @MethodValidation
    def validatePoolName: ValidationResult = CommonMethodValidations.validateName("pool_name", pool_name)
  }

  case class GetCurrencyRequest(
                                 @RouteParam currency_name: String,
                                 @RouteParam pool_name: String,
                                 request: Request
                               ) extends RequestWithUser with WithPoolInfo {
    @MethodValidation
    def validatePoolName: ValidationResult = CommonMethodValidations.validateName("pool_name", pool_name)
  }

}

