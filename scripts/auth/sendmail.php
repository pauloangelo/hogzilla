<?php
require_once 'class.phpmailer.php';

$SMTP="127.0.0.1";
$PORTASMTP="25";
$EMAILREMETENTE="security@company....";
$NOMEREMETENTE="Security Team";
$ASSUNTO="Security Alert: ...";

$listaemails=file("sendlist.txt");
foreach ($listaemails as $linhaString )
{
    $linha = explode("|",$linhaString);
    // UA|fjanku|Windows 8/Chrome|Campinas/Sao Paulo/BR|Windows 8/Chrome
    $type     = $linha[0];
    $username = $linha[1];
    $date     = $linha[2];
    $ip       = $linha[3];
    $location = $linha[4];
    $ua       = $linha[5];
    //$to       = "$username@domain.com";
    $to       = "test-first@domain.com";

    $mail = new PHPMailer();

    $body = file_get_contents("$type.html");

    $body = str_replace('[USERNAME]',$username,$body);
    $body = str_replace('[UA]',$ua,$body);
    $body = str_replace('[LOCATION]',$location,$body);
    $body = str_replace('[DATE]',$date,$body);
    $body = str_replace('[IP]',$ip,$body);

    $mail->IsSMTP(); 
    $mail->Host       = "$SMTP";
    $mail->Port       = "$PORTASMTP";
    
    $mail->SetFrom($EMAILREMETENTE, $NOMEREMETENTE);
    //$mail->AddReplyTo($REPLYTO,$REPLYTONOME);
    $mail->Subject    = $ASSUNTO;
    
    $mail->AltBody    = "use a HTML client"; // optional, comment out and test
    $mail->CharSet = "UTF-8";
    $mail->MsgHTML($body);
    //$mail->AddEmbeddedImage('cab.jpg', 'cab');
    //$mail->AddAttachment("imagem/phpmailer_mini.gif"); // attachment
    
    $mail->AddAddress($to);
    if(!$mail->Send()) {
            //echo "Mailer Error: " . $mail->ErrorInfo;
            //print_r($mail);
    } else {
            //echo "Message sent! to $to\n";
    }

}
?>
