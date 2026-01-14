$client = [System.Net.Sockets.TcpClient]::new('localhost', 9092)
$stream = $client.GetStream()

# Hex request: 00000023 0012 0004 6f7fc661 0009 6b61666b612d636c69 000a 6b61666b612d636c69 04302e3100
$hex = "0000001a0012000467890abc00096b61666b612d636c69000a6b61666b612d636c6904302e3100"
$bytes = [byte[]]::new($hex.Length / 2)
for ($i = 0; $i -lt $hex.Length; $i += 2) {
    $bytes[$i / 2] = [Convert]::ToByte($hex.Substring($i, 2), 16)
}

$stream.Write($bytes, 0, $bytes.Length)
$stream.Flush()
Start-Sleep -Milliseconds 500

$buffer = New-Object byte[] 1024
$stream.ReadTimeout = 2000
try {
    $read = $stream.Read($buffer, 0, 1024)
    Write-Host "Received $read bytes:"
    Write-Host ([BitConverter]::ToString($buffer[0..($read-1)]))
} catch {
    Write-Host 'Timeout or no response'
}
$client.Close()
