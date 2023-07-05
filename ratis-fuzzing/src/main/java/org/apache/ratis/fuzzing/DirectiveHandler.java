// package org.apache.ratis.server.impl.comm;

// import com.google.gson.JsonParser;
// import io.netty.buffer.ByteBuf;
// import io.netty.handler.codec.http.*;
// import io.netty.util.CharsetUtil;
// import io.github.netrixframework.DirectiveExecutor;

// import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
// import static io.netty.handler.codec.http.HttpHeaderValues.APPLICATION_JSON;

// public class DirectiveHandler implements Handler{

//     DirectiveExecutor executor;

//     public DirectiveHandler(DirectiveExecutor executor) {
//         this.executor = executor;
//     }
//     @Override
//     public FullHttpResponse handle(FullHttpRequest req) {
//         ByteBuf content = req.content();
//         if(content == null || content.readableBytes() <= 0) {
//             return new DefaultFullHttpResponse(
//                     req.protocolVersion(),
//                     HttpResponseStatus.BAD_REQUEST
//             );
//         }
//         if(!req.headers().get(CONTENT_TYPE).equals(APPLICATION_JSON.toString())) {
//             return new DefaultFullHttpResponse(
//                     req.protocolVersion(),
//                     HttpResponseStatus.BAD_REQUEST
//             );
//         }
//         try {
//             String directiveJson = content.toString(CharsetUtil.UTF_8);
//             String action = JsonParser.parseString(directiveJson).getAsJsonObject().get("action").getAsString();

//             switch (action) {
//                 case "START":
//                     executor.start();
//                     break;
//                 case "STOP":
//                     executor.stop();
//                     break;
//                 case "RESTART":
//                     executor.restart();
//                     break;
//             }
//             return new DefaultFullHttpResponse(
//                     req.protocolVersion(),
//                     HttpResponseStatus.OK
//             );
//         } catch (Exception e) {

//         }
//         return new DefaultFullHttpResponse(
//                 req.protocolVersion(),
//                 HttpResponseStatus.INTERNAL_SERVER_ERROR
//         );
//     }
// }
