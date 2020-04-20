package com.comcast.keb.addressstandardize;

import com.comcast.keb.addressstandardize.config.CommonProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.http.*;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
@Component

public class NaxService {

    @Autowired
    private RestTemplate restTemplate;
    @Autowired
    private CommonProperties commonProperties;
    @Bean
    public RestTemplate rest() {
        RestTemplate restTemplate = new RestTemplate();
        MappingJackson2HttpMessageConverter converter = new MappingJackson2HttpMessageConverter();
        converter.setObjectMapper(new ObjectMapper());
        restTemplate.getMessageConverters().add(new MappingJackson2HttpMessageConverter());
        restTemplate.getMessageConverters().add(converter);
        restTemplate.setErrorHandler(new ResponseErrorHandler() {
            @Override
            public boolean hasError(ClientHttpResponse response) throws IOException {
                return false;
            }

            @Override
            public void handleError(ClientHttpResponse response) throws IOException {
            }
        });
        return restTemplate;
    }
    public  AddressResponse getNaxResponse(AddressRequest addressRequest) throws Exception {
        String url = commonProperties.getNAXurl();

        HttpEntity entity = buildRequestHeaders(addressRequest);
        ResponseEntity<AddressResponse>  addressResponse = restTemplate.exchange(url, HttpMethod.POST,entity, AddressResponse.class);


        if (addressResponse.getStatusCode().equals(HttpStatus.OK)) {
            return addressResponse.getBody();
        } else {
            throw new Exception(addressResponse.getStatusCode().toString());
        }
    }


    private  HttpEntity buildRequestHeaders(@RequestBody AddressRequest addressRequest) throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Content-Type", "application/json");
        ObjectMapper mapper = new ObjectMapper();
        String jsonString = mapper.writeValueAsString(addressRequest).toString();
        return new HttpEntity(jsonString, headers);
    }
}
